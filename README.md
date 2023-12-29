import copy
import datetime

import hashlib
import json
from collections import defaultdict
from datetime import timedelta
from itertools import chain
from itertools import groupby
from operator import itemgetter
from copy import deepcopy
from time import gmtime
from typing import Any, Dict, List, Tuple, Optional, Iterable, Union

import pytz
from django import forms
from django.conf import settings
from django.contrib.staticfiles.finders import find
from django.core.exceptions import ObjectDoesNotExist
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.db import transaction
from django.db.models import Q, F, Prefetch
from django.db.models.expressions import RawSQL
from django.forms.models import modelform_factory
from django.http import HttpResponseRedirect, Http404
from django.utils import timezone, six
from django.utils.encoding import force_text
from django.utils.functional import cached_property
from django.utils.timezone import now as timezone_now, utc
from django.utils.translation import ugettext as _, ugettext_lazy as __
from shapely import geometry as shapely_geometry
from six.moves.urllib.parse import urlencode
from vist_core.forms import ColorField
from vist_core.generics import AngularDictionaryView, AngularRestView
from vist_core.generics.control_report import ReportView, ReportingMixin, QueryReport
from vist_core.generics.logging import action_log
from vist_core.generics.stereotype_dictionary import FormRenderer, StereotypeDictionary, CellTemplates as CT
from vist_core.logic.enterprise import get_core_enterprise_settings, get_core_enterprise_settings_by_ids, ENTERPRISE_ID
from vist_core.logic.idle import IdleTypeTree as IdleTypeTreeLogic
from vist_core.logic.idle.tree import IdleTypeTree as ITTreeLogic
from vist_core.logic.user import User
from vist_core.logic.work_regime import get_current_regime_detail
from vist_core.logic.stoppage_nurse import StoppageNurse
from vist_core.models import Geometry, VehiclePart, VehicleKindPart, Role, Employee, UserProfile, \
    LoadTypeToFMCode
from vist_core.models.idle import IdleTypeCodes, IdleTypeIntervals
from vist_core.models.idle import (OrganizationCategory, AnalyticCategory, IdleType, IdleVehicleKind, IdleTypeNorming,
                                   Idle, IDLE_TYPE_CODES, IdleTypeToFMCode, SecondaryOperation,
                                   SecondaryOperationNorming, VehicleShiftIdle, VehicleFullShiftIdle, NORMINGS, TYPES,
                                   IdlePositions, IDLE_PLAN_TYPE, IdleTypeTag, IdleTypeTree,
                                   IdleTypeTreeRolePerm, IdleTypeTreeDivisionPerm, IdleTypeTreeEmployeePerm)
from vist_core.models.organization import Division, Enterprise
from vist_core.models.service import ShortLinks
from vist_core.models.vehicle import VEHICLE_KINDS, Vehicle, VehicleType, Model
from vist_core.models.work_regime import WorkRegimeDetail
from vist_core.models import Employee, Person
from vist_core.logic.work_regime import get_regime_list_for_time_range
from vist_core.reporting.controls import (
    EnterprisesControl,
    EnterprisesControlRB,
    VehiclesControlRB,
    VehicleKindControl,
    DateControl,
    IdleDurationControl,
    IdleMaxDurationControlV2,
    WorkRegimeDetailControl,
    WorkRegimeDetailControlRB,
    DateTimeRangeUberControlRB,
)
from vist_core.utils import Context
from vist_core.utils.geodistance import Point
from vist_core.utils.report_utils import HHMMSSMixin
from vist_core.utils.timeutil import TIME_FORMAT
from vist_core.services.idle_splitter import IdleSplitter
from vist_underground.models import PlanMessage, PlanMessageSBU
from vist_underground.caches.idle import idles_cache


PIT_IMPORTED = 'vist_pit' in settings.INSTALLED_APPS
if PIT_IMPORTED:
    from vist_pit.logic import get_enterprise_setting_value
    from vist_pit.utils.inoperability import InoperabilityRangesMixin

__all__ = (
    "AnalyticCategoryView",
    "OrganizationCategoryView",
    "IdleTypeDictView",
    "IdleReportView",
    "IdleReasonEditView",
    "ShiftVehicleReadinessView",
    "ShiftVehicleReadinessInnerIdleView",
    "ShiftVehicleReadinessFullIdleView",
    "IdleTypeToFMView",
    'IdleTypeIntervalsView',
    "IdleTypeTreeView",
    "IdleTypeTagView",
)

# настройка использовать ли честный вариант работы с простоями или как и раньше
USE_TRUE_IDLES = getattr(settings, 'USE_TRUE_IDLES', False)
CALC_IDLE_GEOMETRIES = getattr(settings, 'CALC_IDLE_GEOMETRIES', True)
SHOW_VEHICLE_PARTS = getattr(settings, 'SHOW_VEHICLE_PARTS', False)
ALLOW_AUTO_IDLE_DELETE = getattr(settings, 'ALLOW_AUTO_IDLE_DELETE', False)


class AnalyticCategoryForm(forms.ModelForm):
    class Meta(object):
        model = AnalyticCategory
        fields = ['name', 'code', 'decreases_ktg', 'decreases_ki', 'decreases_worktime', 'decreases_operation_time',
                  'decreases_efficiency', 'production', 'exit_from_work', 'custom']


class AnalyticCategoryView(AngularDictionaryView):
    name = "core_analytic_category_view"
    menu_name = _("Справочник аналитических категорий")
    verbose_name = _('Справочник аналитических категорий')
    description = _('Справочник аналитических категорий типов простоев')
    context_object_name = "analytic_categories"
    template_name = "idle_analytic_category.jinja"
    create_form_class = AnalyticCategoryForm
    update_form_class = AnalyticCategoryForm

    def get_form_kwargs(self):
        kwargs = super(AnalyticCategoryView, self).get_form_kwargs()
        if not self.object_id:
            kwargs.update(instance=AnalyticCategory(
                enterprise_id=self.enterprise.id))
        return kwargs

    def get_queryset(self):
        if self.enterprise:
            return AnalyticCategory.objects.filter(enterprise_id=self.enterprise.id)
        return AnalyticCategory.objects.none()


class OrganizationCategoryForm(forms.ModelForm):

    class Meta(object):
        model = OrganizationCategory
        fields = ['name', 'code']

    def validate_enterprise_unique(self, field, name):

        enterprise_id = self.instance.enterprise_id
        f = self.cleaned_data.get(field)
        if f:
            if OrganizationCategory.objects.filter(**{"enterprise_id": enterprise_id, field: f}).exclude(id=self.instance.id).exists():
                raise ValidationError(_("%s - не уникальный атрибут") % name)
        return f

    def clean_name(self):
        return self.validate_enterprise_unique("name", _('Название'))


class OrganizationCategoryView(StereotypeDictionary):
    name = "core_organization_category_view"
    verbose_name = _('Справочник организационных категорий')
    model = OrganizationCategory
    update_form_class = create_form_class = OrganizationCategoryForm

    column_defs = [
        CT.Default('name', _('Название')),
        CT.Default('code', _('Код')),
    ]


class IdleTypeTagForm(forms.ModelForm):

    class Meta(object):
        model = IdleTypeTag
        fields = ['parent', 'name', 'export_code', 'color']


class IdleTypeTagView(ReportingMixin, AngularDictionaryView):
    name = "core_idle_type_tag_view"
    create_form_class = update_form_class = IdleTypeTagForm
    template_name = "idle_type_tag.jinja"
    verbose_name = _("Тэги видов простоев")
    show_header = True
    emulate_delete = False

    controls = {
        'enterprise': EnterprisesControl,
    }

    def create_object(self, create_form, commit=True):
        tag = super(IdleTypeTagView, self).create_object(
            create_form, commit=False)
        tag.enterprise_id = self.enterprise.id
        tag.save()
        return tag

    def get_queryset(self):
        return IdleTypeTag.objects.filter(enterprise_id=self.enterprise.id).order_by('order_key')


class IdleTypeDictForm(forms.ModelForm):

    vehicle_kinds = forms.MultipleChoiceField(
        label=__("Виды техники"), choices=VEHICLE_KINDS.choices, required=False)
    is_engine_working = forms.NullBooleanField(
        label=__("Работает ли двигатель"), widget=forms.CheckboxInput)
    is_maintanence_availabel = forms.NullBooleanField(
        label=__("Доступен в форме тех.готовности"), widget=forms.CheckboxInput)
    color = ColorField(label=__("Цвет"), initial='#888888', required=False)

    class Meta(object):
        model = IdleType
        fields = [
            'name', 'code', 'immutable', 'organization_category', 'analytic_category',
            'division', 'work_time_truck', 'work_time_driver', 'is_engine_working', 'kind',
            'plan_type', 'export_code', 'export_code_2', 'vehicle_type', 'vehicle_model', 'color',
            'is_maintanence_availabel', 'is_work_map_plan_available', 'need_to_cut', 'tag_list',
            'wtm_blocked_basic_work',
        ]

    def __init__(self, *args, **kw):
        PFX = "\u00A0" * 5
        super(IdleTypeDictForm, self).__init__(*args, **kw)
        self.fields['organization_category'].queryset = \
            OrganizationCategory.objects.filter(
                enterprise_id=self.instance.enterprise_id)
        self.fields['analytic_category'].queryset = \
            AnalyticCategory.objects.filter(
                enterprise_id=self.instance.enterprise_id)
        tags_field = self.fields['tag_list']
        tags_field.choices = [(t.id, PFX * (t.level - 1) + t.name,)
                              for t in tags_field.queryset.order_by('order_key')]

    def clean(self):
        cleaned_data = super(IdleTypeDictForm, self).clean()
        cleaned_code = cleaned_data.get('code', None)
        if cleaned_code:
            qs = IdleType.objects.filter(Q(enterprise__isnull=True) | Q(
                enterprise_id=self.instance.enterprise_id), code=cleaned_code)
            if self.instance.id:
                qs = qs.exclude(id=self.instance.id)
            if qs.exists():
                err_msg = _("Вид простоя с этим кодом уже существует")
                self._errors["code"] = self.error_class([err_msg])
                del cleaned_data['code']
        return cleaned_data

    def clean_color(self):
        color = self.cleaned_data.get('color')
        return color if color else '#888888'

    def clean_vehicle_kinds(self):
        vehicle_kinds = self.cleaned_data['vehicle_kinds']
        if list(six.moves.filter(lambda k: k not in VEHICLE_KINDS, vehicle_kinds)):
            raise ValidationError(_("Указан неверный тип техники"))
        return vehicle_kinds

    def save(self, commit=True):
        obj = super(IdleTypeDictForm, self).save(commit=commit)

        def save_kinds():
            # сохранение связей с видами техники
            IdleVehicleKind.objects.filter(idle_type=obj.id).delete()
            IdleVehicleKind.objects.bulk_create([
                IdleVehicleKind(idle_type=obj, vehicle_kind_code=kind_code)
                for kind_code in self.cleaned_data['vehicle_kinds']
            ])
        # финт, чтобы флажок commit учитывался (по аналогии с джангой)
        if commit:
            save_kinds()
        else:
            def save_m2m_and_vehicle_kinds():
                save_m2m = getattr(self, 'save_m2m', None)
                if save_m2m:
                    save_m2m()
                save_kinds()
            self.save_m2m = save_m2m_and_vehicle_kinds
        return obj


class IdleTypeIntervalsForm(forms.ModelForm):
    class Meta(object):
        model = IdleTypeIntervals
        fields = ['time_begin', 'time_end', 'idle_type', 'enterprise']

    def clean(self):
        cleaned_data = super().clean()
        time_begin = cleaned_data.get("time_begin")
        time_end = cleaned_data.get("time_end")
        if time_end == time_begin:
            raise ValidationError(
                f"Нельзя вводить одинаковое начало и конец {time_end}")

        if time_begin > time_end:
            # диапазон 20:00-04:00 взят из головы наугад, но вроде разумно
            if time_begin < timedelta(hours=20) or time_end > timedelta(hours=4):
                raise ValidationError(
                    f"Скорее всего вы ошиблись с вводом начала={time_begin} и конца={time_end}")
            cleaned_data["time_end"] += timedelta(hours=24)
        return cleaned_data


class IdleTypeIntervalsView(StereotypeDictionary):
    name = 'core_idle_type_interval'
    model = IdleTypeIntervals
    template_name = "idle_type_interval.jinja"
    verbose_name = _("Интервалы возникновения нормативных простоев")
    create_form_class = update_form_class = IdleTypeIntervalsForm
    menu_name = _("Интервалы возникновения нормативных простоев")

    column_defs = [
        CT.Default("time_begin_str", _("Время начала интервала")),
        CT.Default("time_end_str", _("Время Окончания интервала")),
    ]

    def serialize_obj(self, obj):
        obj = super(IdleTypeIntervalsView, self).serialize_obj(obj)
        # for field in filter(lambda x: x.get('duration'), self._get_datafields()):
        #     if obj.get(field['name']):
        #         value = human_time(obj[field['name']])
        #         obj[field['name']] = value[0:len(value) - 3]
        return obj


class IdleTypeDictFormRenderer(FormRenderer):
    label_width = 4
    field_width = 8


class IdleTypeDictView(StereotypeDictionary):
    name = "core_idle_type_dict_view"
    verbose_name = __('Справочник типов простоев')
    create_form_class = IdleTypeDictForm
    update_form_class = IdleTypeDictForm
    model = IdleType
    create_form_template = update_form_template = "dictionaries/idle_types.jinja"
    search_enabled = True
    form_render = IdleTypeDictFormRenderer

    column_defs = [
        CT.Default("name", __("Название")),
        CT.Default("organization_category_name",
                   __("Организационная категория")),
        CT.Default("analytic_category_name", __("Аналитическая категория")),
        CT.Default("code", __("Код")),
        CT.Default("plan_type_name", __("Плановый")),
        CT.Default("division_name", __("Ответственное подразделение")),
        CT.Default("vehicle_kinds_verbose", __("Вид техники")),
        CT.Default("vehicle_type_verbose", __("Тип техники")),
        CT.Default("vehicle_model_verbose", __("Модель техники")),
        CT.Default("export_code", __("Экспортный код")),
        CT.Default("export_code_2", __("Экспортный код(SAP)")),
        CT.Default("kind_name", __("Вид")),
        CT.Bool("is_maintanence_availabel", __(
            "Доступен в форме тех.готовности")),
        CT.Bool("is_work_map_plan_available", __(
            "Доступен в оперативном плане КРВ")),
        CT.Bool("need_to_cut", __("Обрезать по периодическому классификатору")),
        CT.Bool("wtm_blocked_basic_work", __("Блокирует основную работу")),
        CT.Color('color', __('Цвет индикатора')),
        CT.Default('tag_list__verbose', __('Список тэгов')),
    ]
    select_related = ["organization_category", "analytic_category", "division"]
    prefetch_related = ['tag_list']

    @cached_property
    def vehicle_kinds(self):
        qs = IdleVehicleKind.objects.filter(
            idle_type__enterprise_id=self.enterprise_id)
        res = defaultdict(list)
        for vk in qs:
            res[vk.idle_type_id].append(vk.vehicle_kind_code)
        return res

    @cached_property
    def vehicle_kinds_verbose(self):
        return dict(VEHICLE_KINDS.choices)

    def get_extra_fields(self, obj_dict):
        obj_dict["vehicle_kinds"] = self.vehicle_kinds[obj_dict["id"]]
        obj_dict["vehicle_kinds_verbose"] = ", ".join([force_text(self.vehicle_kinds_verbose[code])
                                                       for code in obj_dict["vehicle_kinds"]
                                                       if code in self.vehicle_kinds_verbose])
        obj_dict["vehicle_type_verbose"] = ", ".join(VehicleType.objects.filter(
            id__in=obj_dict["vehicle_type"]).values_list('name', flat=True))
        obj_dict["vehicle_model_verbose"] = ", ".join(Model.objects.filter(
            id__in=obj_dict["vehicle_model"]).values_list('name', flat=True))
        obj_dict["is_system"] = obj_dict["code"] in IDLE_TYPE_CODES
        obj_dict['plan_type_name'] = IDLE_PLAN_TYPE.get(obj_dict['plan_type'])

    def get_organization_category_queryset(self):
        return OrganizationCategory.objects.filter(enterprise_id=self.enterprise_id)

    def get_analytic_category_queryset(self):
        return AnalyticCategory.objects.filter(enterprise_id=self.enterprise_id)

    def get_division_queryset(self):
        return Division.objects.filter(enterprise_id=self.enterprise_id).order_by('name')

    def get_vehicle_type_queryset(self):
        return VehicleType.objects.filter(enterprise_id=self.enterprise_id)

    def get_vehicle_model_queryset(self):
        return Model.objects.filter(enterprise_id=self.enterprise_id)

    def get_tag_list_queryset(self):
        return IdleTypeTag.objects.filter(enterprise_id=self.enterprise_id)

    def delete(self):
        code = self.instance.code
        if self.is_system_code(code):
            return self.j({'non_field_errors': _('Невозможно удалить системный вид простоя.'),
                           'errors': []}, status=400)
        return super(IdleTypeDictView, self).delete()

    def get_system_idle_type_codes(self):
        return sorted((v) for k, v in IdleTypeCodes.__dict__.items() if not k.startswith('__'))

    def is_system_code(self, code):
        try:
            code = int(code)
        except (ValueError, TypeError):
            return False
        return code in self.get_system_idle_type_codes()

    def get_context_data(self, **kwargs):
        context = super(IdleTypeDictView, self).get_context_data(**kwargs)
        context['IDLE_TYPE_CODES'] = sorted(
            list(six.iteritems(IDLE_TYPE_CODES)), key=itemgetter(0))[1:]
        return context


class IdleTypeTreeForm(forms.ModelForm):

    def clean_plan_type(self):
        plan_type = self.cleaned_data['plan_type']
        if plan_type not in IDLE_PLAN_TYPE:
            plan_type = IDLE_PLAN_TYPE.DUAL
        return plan_type

    def clean_color(self):
        color = self.cleaned_data['color']
        if not color:
            color = "#888888"
        return color

    def clean(self):
        parent = self.cleaned_data['parent']
        name = self.cleaned_data['name']
        idle_type = self.cleaned_data['idle_type']
        if name and idle_type and IdleTypeTree.objects \
            .filter(parent=parent, name=name, idle_type=idle_type,) \
                .exclude(id=self.instance.id).exists():
            raise ValidationError(__("Данное значение уже существует"))
        return self.cleaned_data

    class Meta(object):
        model = IdleTypeTree
        fields = ['parent', 'name', 'export_code',
                  'color', 'plan_type', 'idle_type']


class IdleTypeTreeView(ReportingMixin, AngularDictionaryView):
    name = "core_idle_type_tree_view"
    create_form_class = update_form_class = IdleTypeTreeForm
    template_name = "idle_type_tree.jinja"
    verbose_name = _("Классификация видов простоев")
    show_header = True
    emulate_delete = False

    get_actions = ['get_permission']
    post_actions = ['set_permission']

    controls = {
        'enterprise': EnterprisesControl,
    }

    def create_object(self, create_form, commit=True):
        obj = super(IdleTypeTreeView, self).create_object(
            create_form, commit=False)
        obj.enterprise_id = self.enterprise.id
        obj.save()
        return obj

    def get_queryset(self):
        return (
            IdleTypeTree.objects
            .filter(enterprise_id=self.enterprise.id)
            .select_related('idle_type')
            .order_by('order_key')
        )

    def get_context_data(self, **kwargs):

        def get_division_list():
            pfx = "\u00A0" * 5
            division_list = []
            division_dict = {d.id: d for d in Division.objects.filter(
                enterprise_id=self.enterprise.id
            ).order_by('name')}

            def get_order_key(div):
                key = "/%d" % div.id
                if div.parent_id:
                    key = get_order_key(division_dict[div.parent_id]) + key
                return key

            for division in division_dict.values():
                order_key = get_order_key(division)
                level = order_key.count('/')
                division_list.append(dict(
                    id=division.id,
                    name=pfx * level + division.name,
                    order_key=order_key,
                    level=level,
                ))

            division_list.sort(key=lambda d: d['order_key'])
            return division_list

        context = super(IdleTypeTreeView, self).get_context_data(**kwargs)
        context['plan_type_list'] = [{'id': k, 'name': v}
                                     for k, v in six.iteritems(IDLE_PLAN_TYPE)]
        idle_type_list = list(
            IdleType.objects
            .filter(enterprise_id=self.enterprise.id)
            .values('id', 'name', 'organization_category__name', 'plan_type')
        )

        for idle_type in idle_type_list:
            idle_type['name'] = f'{idle_type["name"]} --- {IdleType.IDLE_PLAN_TYPE[idle_type["plan_type"]]}'
            idle_type.pop('plan_type')

        idle_type_list.insert(0, {'id': None, 'name': _(
            "Без вида простоя"), 'organization_category__name': ""})
        context['idle_type_list'] = idle_type_list
        context['role_list'] = (
            Role.objects
            .filter(enterprise_id=self.enterprise.id)
            .order_by('name')
            .values('id', 'name')
        )
        context['division_list'] = get_division_list()
        context['employee_list'] = [
            {'id': e.id, 'name': e.get_name()}
            for e in (
                Employee.objects
                .filter(enterprise_id=self.enterprise.id)
                .select_related('person', 'post')
                .order_by('person__fam_name', 'person__first_name', 'person__second_name')
            )
        ]

        context['idle_tree_permissions'] = self.get_idle_tree_permissions()

        return context

    def get_idle_tree_permissions(self):
        idle_tree_ids = IdleTypeTree.objects.all().values_list('id', flat=True)

        def get_field_by_tree(model: object, field: str):
            tree_and_roles = (
                model.objects
                .all()
                .values_list('tree_id', field)
            )
            field_by_tree = defaultdict(list)
            for tree_id, field in tree_and_roles:
                field_by_tree[tree_id].append(field)
            return field_by_tree

        roles_by_tree = get_field_by_tree(IdleTypeTreeRolePerm, 'role_id')
        division_by_tree = get_field_by_tree(
            IdleTypeTreeDivisionPerm, 'division_id')
        employee_by_tree = get_field_by_tree(
            IdleTypeTreeEmployeePerm, 'employee_id')
        idle_tree_permissions = {
            idle_tree_id: {
                'who_i_am': 'tree_id',
                'roles_ids': roles_by_tree[idle_tree_id],
                'divisions_ids': division_by_tree[idle_tree_id],
                'employees_ids': employee_by_tree[idle_tree_id],
            }
            for idle_tree_id in idle_tree_ids
        }
        return idle_tree_permissions

    @cached_property
    def idle_tree(self):
        return self.get_queryset().get(pk=self.data.id)

    def get_permission(self, request):

        def get_perm_list(perm_model, perm_field):
            return perm_model.objects.filter(tree=self.idle_tree).values_list(perm_field, flat=True)

        perm = {
            'id': self.idle_tree.id,
            'role_list': get_perm_list(IdleTypeTreeRolePerm, 'role'),
            'division_list': get_perm_list(IdleTypeTreeDivisionPerm, 'division'),
            'employee_list': get_perm_list(IdleTypeTreeEmployeePerm, 'employee'),
        }
        return self.j(perm)

    def set_permission(self, request):

        def set_perm(perm_model, perm_field, perm_list):
            perm_model.objects.filter(tree=self.idle_tree).delete()
            perm_model.objects.bulk_create([
                perm_model(**{'tree': self.idle_tree, "%s_id" % perm_field: v}) for v in perm_list
            ])

        set_perm(IdleTypeTreeRolePerm, 'role', self.data.role_list)
        set_perm(IdleTypeTreeDivisionPerm, 'division', self.data.division_list)
        set_perm(IdleTypeTreeEmployeePerm, 'employee', self.data.employee_list)

        return self.j({})


class NormingForm(forms.Form):
    idle_type = forms.ChoiceField(label=_('Вид простоя'))
    norming_type = forms.ChoiceField(
        choices=IdleTypeNorming.NORMING_TYPES, label=_('Тип нормирования'))

    def __init__(self, idle_type_choices, *args, **kwargs):
        super(NormingForm, self).__init__(*args, **kwargs)
        self.fields['idle_type'].choices = idle_type_choices
        self.fields['idle_type'].widget.attrs['ng-disabled'] = "CD.object.id"
        self.fields['norming_type'].widget.attrs['ng-disabled'] = "CD.object.id"


class FormValidationError(ValueError):
    """ругаться формой, чтобы потом было на кого свалить"""

    def __init__(self, form):
        self.form = form


class IdleTypeNormingDictView1(StereotypeDictionary):
    """
    Справочник на две модели: IdleTypeNorming и SecondaryOperationNorming
    модель определяется по префиксу ('idle_type' и 'operation' соответственно)
    редактирование происходит не по одной записи, как обычно, а списком. причем,
    для всего списка указывается единые Тип простоя и Тип нормирования
    """
    name = "core_idle_type_norming"
    verbose_name = _("Справочник нормативов на виды простоев")
    template_name = 'dictionaries/idle_type_norming.jinja'
    create_form_class = update_form_class = NormingForm
    column_defs = [
        CT.Default("idle_type__name", _("Тип простоя")),
        CT.Default("norming_type__name", _("Тип нормирования")),
        # CT.Default("interval_type", _("Интервал возникновения")),
    ]
    # prefetch_related = ["interval_type"]

    # def get_interval_type_queryset(self):
    #     return IdleTypeIntervals.objects.all()

    def to_dict(self, obj):
        if isinstance(obj, dict):
            return obj
        return super(IdleTypeNormingDictView1, self).to_dict(obj)

    @cached_property
    def norming_types(self):
        return dict(NORMINGS)

    @property
    def PREFIX(self):
        """
        просто избавиться от путанницы со строками
        """
        class Prefix:
            idle_type = 'idle_type'
            operation = 'operation'
        return Prefix

    def norming_to_dict(self, norm, **kwargs):
        """
        сериализация нормативов на простои
        """
        try:
            params = json.loads(norm.params)
        except (TypeError, ValueError):
            params = None
        return dict(
            id=norm.id,
            value=norm.value,
            params=params,
            **kwargs
        )

    def idle_type_to_dict(self, idle_type, norming_type, norms, prefix):
        """
        сериализация нормативов, сгрупированных по типу простоя
        """
        normings = [self.norming_to_dict(n, index=i+1)
                    for i, n in enumerate(norms)]
        del_suffix = '*' if idle_type.is_deleted else ''
        intervals = [self.intervals_to_dict(n, norming_list=[(norm['id'], norm['index']) for norm in normings])
                     for n in self.get_instance_interval_list(idle_type.id)]
        return dict(
            type=prefix,
            id='%s_%s' % (prefix, idle_type.id),
            idle_type='%s_%s' % (prefix, idle_type.id),
            idle_type__name=idle_type.name + del_suffix,
            norming_type=norming_type,
            norming_type__name=self.norming_types.get(norming_type, ''),
            normings=normings,
            intervals=intervals,
        )

    def get_norming_queryset(self, prefix):
        qs = {
            self.PREFIX.idle_type:
                IdleTypeNorming.objects.select_related("idle_type").filter(
                    idle_type__enterprise_id=self.enterprise.id),

            self.PREFIX.operation:
                SecondaryOperationNorming.objects.select_related("operation")
                .filter(operation__enterprise_id=self.enterprise.id),
        }[prefix]
        return qs

    def get_instance_list(self, prefix, idle_type_id, norming_type):
        """
        список нормативов для указанного типа простоя и типа нормирования
        """
        flt_kw = {
            self.PREFIX.idle_type: dict(idle_type_id=idle_type_id, norming_type=norming_type),
            self.PREFIX.operation: dict(operation_id=idle_type_id, norming_type=norming_type),
        }[prefix]
        return self.get_norming_queryset(prefix).filter(**flt_kw)

    def get_idle_type_norming_list(self):
        """
        получить сериализованный сгруппированный список 'Нормативы на виды простоев'
        """
        idle_type_norming_list = []
        idle_qs = self.get_norming_queryset(self.PREFIX.idle_type)
        idle_norming = defaultdict(list)
        for norm in idle_qs:
            idle_norming[(norm.idle_type, norm.norming_type)].append(norm)
        for (idle_type, norming_type), norms in sorted(list(idle_norming.items()), key=lambda x: (x[0][0].name, x[0][1])):
            idle_type_norming_list.append(
                self.idle_type_to_dict(
                    idle_type, norming_type, norms, self.PREFIX.idle_type)
            )

        def get_sort_key(keys, id):
            for key in keys:
                if key[0] == id:
                    return key[1]
            return 0

        for norm in idle_type_norming_list:
            if not norm['intervals']:
                continue
            # Здесь хранится порядок интервалов
            keys = norm['intervals'][0]['norming_list']
            for interval in norm['intervals']:
                interval['sort_key'] = get_sort_key(
                    keys, interval.get('normings'))
            norm['intervals'] = sorted(
                norm['intervals'], key=lambda el: el['sort_key'])

        return idle_type_norming_list

    def get_secondary_operation_norming(self):
        """
        получить сериализованный сгруппированный список 'Нормативы на вспомогательные операции'
        """
        secondary_operation_norming = []
        operation_qs = self.get_norming_queryset(self.PREFIX.operation)
        operation_norming = defaultdict(list)
        for norm in operation_qs:
            operation_norming[(norm.operation, norm.norming_type)].append(norm)
        for (operation, norming_type), norms in operation_norming.items():
            secondary_operation_norming.append(
                self.idle_type_to_dict(
                    operation, norming_type, norms, self.PREFIX.operation)
            )
        return secondary_operation_norming

    def get_queryset(self):
        return sorted(chain(
            self.get_idle_type_norming_list(),
            self.get_secondary_operation_norming()
        ), key=lambda o: o['idle_type__name'])

    def get_update_norm_forms(self, prefix, norming_type, obj_norms, db_norms):
        """
        определяем какие нормативы нужно обновить,
        натравливаем их на формы и валидируем.
        возвращается список форм для последующего сохранения
        """
        norm_update_form_class = {
            self.PREFIX.idle_type:
                modelform_factory(model=IdleTypeNorming, fields=[
                                  'norming_type', 'value', 'params']),
            self.PREFIX.operation:
                modelform_factory(model=SecondaryOperationNorming, fields=[
                                  'norming_type', 'value', 'params']),
        }[prefix]

        to_update = [n for n in obj_norms if 'id' in n]
        to_update_forms = []
        for norm in to_update:
            form_data = norm.copy()
            form_data['norming_type'] = norming_type
            if 'params' in form_data:
                form_data['params'] = json.dumps(
                    form_data['params']) if form_data['params'] is not None else None
            form = norm_update_form_class(
                data=form_data, instance=db_norms[norm['id']])
            if form.is_valid():
                form.index = norm.get('index')
                to_update_forms.append(form)
            else:
                raise FormValidationError(form)
        return to_update_forms

    def get_create_norm_forms(self, prefix, type_id, norming_type, obj_norms):
        """
        определяем какие нормативы нужно создать,
        натравливаем их на формы и валидируем.
        возвращается список форм для последующего сохранения
        """
        norm_create_form_class = {
            self.PREFIX.idle_type:
                modelform_factory(model=IdleTypeNorming, fields=[
                                  'idle_type', 'norming_type', 'value', 'params']),
            self.PREFIX.operation:
                modelform_factory(model=SecondaryOperationNorming, fields=[
                                  'operation', 'norming_type', 'value', 'params']),
        }[prefix]

        to_create = [n for n in obj_norms if not n.get('id')]
        to_create_forms = []
        for norm in to_create:
            form_data = norm.copy()
            form_data[prefix] = type_id
            form_data['norming_type'] = norming_type
            if 'params' in form_data:
                form_data['params'] = json.dumps(form_data['params'])
            form = norm_create_form_class(data=form_data, instance=None)
            if form.is_valid():
                form.index = norm.get('index')
                to_create_forms.append(form)
            else:
                raise FormValidationError(form)
        return to_create_forms

    def save_norms(self):
        """
        обновить данные в соответствии с присланным
        """
        obj = self.data.get("object", dict())
        norming_form = NormingForm(self.get_idle_type_choices(), data=obj)
        if norming_form.is_valid():
            norming_type = int(norming_form.cleaned_data['norming_type'])
            prefix, type_id = norming_form.cleaned_data['idle_type'].rsplit(
                '_', 1)
        else:
            return self.validation_error_response(norming_form)

        assert prefix in [self.PREFIX.idle_type, self.PREFIX.operation]

        obj_norms = obj.get('normings', [])
        if not obj_norms:
            return self.j(dict(
                non_field_errors=_('Список нормативов не может быть пустым'),
                errors=[]
            ), status=400)

        # список нормативов для указанного типа простоя и типа нормирования
        norm_qs = self.get_instance_list(prefix, type_id, norming_type)

        # с точки зрения клиента нельзя создавать с тем же типом простоя, можно редактировать
        action = self.data.get('action')
        if action == 'create' and norm_qs.exists():
            return self.j(dict(
                non_field_errors=_(
                    'Данный тип простоя и тип нормирования уже существует'),
                errors=[]
            ), status=400)

        db_norms = {norm.id: norm for norm in norm_qs}

        # идентификаторы норм для удаления
        to_delete_ids = set(db_norms.keys()) - \
            set([n.get('id') for n in obj_norms if 'id' in n])

        try:
            to_update_forms = self.get_update_norm_forms(
                prefix, norming_type, obj_norms, db_norms)
            to_create_forms = self.get_create_norm_forms(
                prefix, type_id, norming_type, obj_norms)
        except FormValidationError as e:
            return self.validation_error_response(e.form)

        # Если сохраняем IdleType - то нужна часть с сохранением интервалов
        if prefix == self.PREFIX.idle_type:
            raw_intervals = obj.get('intervals', [])
            try:
                created_intervals = self.save_intervals(
                    norming_type=norming_type)
            except FormValidationError as e:
                return self.validation_error_response(e.form)

            # Если интервалы пришли с фронта, но мы не создали и не изменили ни один интервал - ошибка
            if raw_intervals and not created_intervals:
                return self.j(dict(
                    non_field_errors=_(
                        'Ошибка записи. Имеются пересечения в интервалах возникновения простоев'),
                    errors=[]
                ), status=400)

        # пытаемся обновить данные в БД
        with transaction.atomic():
            # Удалим все интервали вначале, так проще чем сверять правильные интервалы у норм и т.д.
            norm_qs.filter(id__in=to_delete_ids).delete()

            all_objs = []
            norm_by_index = {}
            norm_by_id = {}
            for form in to_update_forms + to_create_forms:
                obj = form.save()
                norm_by_index[form.index] = obj
                norm_by_id[obj.id] = obj
                all_objs.append(obj)

            # еще здесь по факту создания/удаления идет инвалидация кеша ShiftChangeNorms ч/з
            # обработчик сигнала vist_core.signals.invalidate_redis_cached
        # Если сохраняем IdleType - то нужна часть с сохранением интервалов, именно тут
        if prefix == self.PREFIX.idle_type:
            # Сейчас созданные интервалы привязыаем к конкртеной норме или ко всем (зависит от normings)
            # только по созданным сейчас - потому что уже созданные - уже обновили
            for interval_instance in created_intervals:
                interval_origin = interval_instance.source
                current_norm = None
                if 'id' in interval_origin:
                    norm_id = interval_origin['normings']
                    current_norm = norm_by_id[norm_id]
                else:
                    norm_index = interval_origin['normings']
                    current_norm = norm_by_index[norm_index]
                if current_norm:
                    current_norm.interval_type.add(interval_instance)

        # собираем ответ для обновления данных на клиенте
        idle_type = self.get_idle_type_qs(prefix).get(pk=type_id)
        ret_obj = self.idle_type_to_dict(
            idle_type, norming_type, all_objs, prefix)
        return self.success_response(ret_obj)

    def get_create_interval_instances(self, obj_intervals):
        # type: (List) -> List[IdleTypeIntervalsForm]
        """ Получить список провалидированых форм, для создания интервалов """
        to_create = obj_intervals
        created_objs = []
        for interval in to_create:
            form_data = interval.copy()
            form = IdleTypeIntervalsForm(data=form_data, instance=None)
            if form.is_valid():
                interval_instance = form.save()
                interval_instance.source = interval
                created_objs.append(interval_instance)
            else:
                raise FormValidationError(form)
        return created_objs

    def get_update_interval_forms(self, obj_intervals, db_intervals, norming_type):
        # type: (List, Iterable[IdleTypeIntervals], int) -> List[IdleTypeIntervalsForm]
        """ Получить список провалидированых форм, для обновления интервалов """
        to_update = [n for n in obj_intervals if 'id' in n]
        to_update_forms = []
        for interval in to_update:
            form_data = interval.copy()
            instance = db_intervals[interval['id']]
            norms_ids = interval.get("normings", [])

            if isinstance(norms_ids, int):
                instance.interval_type.clear()
                instance.interval_type.add(
                    IdleTypeNorming.objects.get(id=norms_ids))
            elif isinstance(norms_ids, (list, type(None))):
                instance.interval_type.add(
                    *self.get_instance_list('idle_type', interval['idle_type'], norming_type))

            form = IdleTypeIntervalsForm(data=form_data, instance=instance)
            if form.is_valid():
                to_update_forms.append(form)
            else:
                raise FormValidationError(form)
        return to_update_forms

    def save_intervals(self, norming_type):
        # type: (int) -> Tuple[List[IdleTypeIntervals], List[IdleTypeIntervals]]
        """ Удаляем все старые интервали и пересоздаем новые """
        obj = self.data.get("object", dict())
        prefix, idle_type_id = obj["idle_type"].rsplit('_', 1)
        intervals = obj.get('intervals', [])

        is_intersect_interval = self.has_overlap(intervals)

        if is_intersect_interval:
            return []
        for interval in intervals:
            interval['idle_type'] = int(idle_type_id)
            interval['enterprise'] = self.enterprise.id
            interval_form = IdleTypeIntervalsForm(interval)
            if interval_form.is_valid():
                idle_type_id = interval_form.cleaned_data['idle_type'].id
            else:
                raise FormValidationError(interval_form)

        intervals_qs = self.get_instance_interval_list(idle_type_id)

        with transaction.atomic():
            intervals_qs.delete()
            created_objs = self.get_create_interval_instances(intervals)

        return created_objs

    def has_overlap(self, intervals):
        """ Сортирует интервалы по нормам и времени для последуюшей группировки по нормам.
        Возвращает True, если есть пересечение интервалов в группах и/или между группами(для всех норм), иначе - False.

        :param intervals: list, Список имеющихся интервалов данного idleType
        :return: bool
        """
        if not intervals:
            return False

        # отсортируем имеющиеся интервалы по времени начала (по возрастанию) и нормам для посл. группировки:
        sorted_intervals = sorted(
            intervals, key=lambda interval: (
                int(interval['normings']) if isinstance(
                    interval.get('normings'), int) else float('inf'),
                int(interval.get('time_begin') or 0))
        )
        map_normings_to_intervals = {}
        for keys, group_intervals in groupby(sorted_intervals, key=lambda interval: interval.get('normings')):
            # конвертируем ключи словаря в тьюпл, чтобы возможно прилетевший список не был мутабельным ключом словаря
            # или None в случае выбора всех норм
            if isinstance(keys, int):
                keys = (keys, )
            elif isinstance(keys, type(None)):
                keys = list({norm.get('id') for norm in self.data.get(
                    'object', dict()).get('normings', [])})
            list_invervals = list(group_intervals)
            map_normings_to_intervals[
                tuple(keys + [None] if isinstance(keys, list) else keys)
            ] = list_invervals  # маппим интервалы к нормам
            is_intersected = self.is_intersect_interval(list_invervals)
            if is_intersected is True:
                return True

            # если ключом является список, это значит что интервал относится ко нескольким нормам
            if isinstance(keys, list):
                for key in keys:  # проверяем пересечения всех норм со всеми
                    for interval in list_invervals:
                        raw_intervals = [interval]
                        raw_intervals.extend(
                            map_normings_to_intervals.get((key,), []))
                        is_intersected = self.is_intersect_interval(
                            raw_intervals)
                        if is_intersected is True:
                            return True

        return False

    def is_intersect_interval(self, intervals):
        """ Проверяет интервалы на наличие пересечений по времени и
        возвращает True, если есть пересечение интервалов, иначе - False.

        :param intervals: list, Список имеющихся интервалов данного idleType
        :return: bool
        """
        if not intervals:
            return False
        # отсортируем имеющиеся интервалы по времени начала (по возрастанию):
        current_intervals = sorted(
            intervals, key=lambda interval: int(interval.get('time_begin') or 0))
        # составляем список интервалов, которые не пересекаются:
        non_intersected_intervals = [current_intervals[0]]
        for interval in current_intervals[1:]:
            # получаем время окончания интервала, который идет последним в списке без пересечений:
            prev_interval_end_time = int(
                non_intersected_intervals[-1].get('time_end') or 86400)
            # сравниваем это время со временем начала рассматриваемого интервала:
            if prev_interval_end_time > int(interval.get('time_begin') or 0):
                return True
            else:
                # если пересечения не обнаружено, добавляем текущий интервал цикла в список непересекающихся:
                non_intersected_intervals.append(interval)
        return False

    @action_log(action_log.ACTIONS.UPDATE)
    def update(self):
        self.model = IdleTypeNorming
        resp = self.save_norms()
        if resp:
            return resp

        raise NotImplementedError

    @action_log(action_log.ACTIONS.CREATE)
    def create(self):
        self.model = IdleTypeNorming
        resp = self.save_norms()
        if resp:
            return resp

        raise NotImplementedError

    @action_log(action_log.ACTIONS.DELETE)
    def delete(self):
        obj = self.data.get("object", dict())
        try:
            self.model = IdleTypeNorming
            prefix, type_id = obj['idle_type'].rsplit('_', 1)
            norming_type = int(obj['norming_type'])
            norm_qs = self.get_instance_list(prefix, type_id, norming_type)
            norm_qs.delete()
        except Exception:
            return self.j(_("Не удалось удалить запись. Попробуйте еще раз и обратитесь в службу техподдержки."), status=500)
        return self.j("OK")

    def get_idle_type_qs(self, prefix):
        return {
            self.PREFIX.idle_type: IdleType.objects.filter(enterprise_id=self.enterprise.id),
            self.PREFIX.operation: SecondaryOperation.objects.filter(enterprise_id=self.enterprise.id),
        }[prefix]

    def get_idle_type_choices(self):
        """
        список доступных для создания типов простоев
        """
        return chain(
            (("idle_type_%s" % norm_type.id, norm_type.name)
             for norm_type in self.get_idle_type_qs(self.PREFIX.idle_type)),
            (("operation_%s" % norm_type.id, norm_type.name)
             for norm_type in self.get_idle_type_qs(self.PREFIX.operation)),
        )

    def get_form_kwargs(self, data=None, instance=None):
        return dict(idle_type_choices=self.get_idle_type_choices(), data=data)

    def get_norm_intervals_for_vehicle_type(self, idles_types, current_vehicle_type_id):
        """
        Выборка нормативов и интервалов для определённого типа МО
        """
        query_idle_type_norms = Q()

        query_idle_type_norms |= Q(idle_type__in=idles_types)
        intervals_prefetch = Prefetch(
            'interval_type',
            queryset=IdleTypeIntervals.objects.all()
        )
        norms = (
            IdleTypeNorming.objects.filter(
                query_idle_type_norms).prefetch_related(intervals_prefetch)
        )

        norms_filtered = []
        # Выбираем из полученных норм, только те, которые для текущего типа МО
        for norm in norms:
            norm: IdleTypeNorming
            params_dict = json.loads(norm.params)
            if params_dict.get('vehicle_type') and params_dict.get('vehicle_type') != current_vehicle_type_id:
                continue
            norms_filtered.append(norm)

        norm_intervals = []
        # Получаем интервалы для отобранных норм
        for idle_norm in norms_filtered:
            for interval in idle_norm.interval_type.all():
                interval: IdleTypeIntervals
                norm_interval = {
                    'time_begin': interval.time_begin,
                    'time_end': interval.time_end,
                    'idle_type': interval.idle_type
                }
                norm_intervals.append(norm_interval)
        norm_intervals.sort(key=lambda n: n['time_begin'])

        # Выделяем свободные интервалы и пересечение норм
        free_interval_name = "Свободный интервал"

        index = 0
        while index < len(norm_intervals) - 1:
            current_norm_interval = norm_intervals[index]
            next_norm_interval = norm_intervals[index + 1]

            current_time_end = current_norm_interval['time_end']
            next_time_begin = next_norm_interval['time_begin']

            if next_time_begin != current_time_end and current_time_end < next_time_begin:
                free_interval = {
                    "time_begin": current_time_end,
                    "idle_type__name": free_interval_name,
                    "time_end": next_time_begin
                }
                norm_intervals.append(free_interval)
                norm_intervals.sort(key=lambda n: n['time_begin'])
                index += 1

            if current_time_end > next_time_begin:
                current_norm_interval['intersection'] = True
                next_norm_interval['intersection'] = True

            if current_norm_interval["idle_type"] == next_norm_interval["idle_type"] and current_time_end == next_time_begin:
                current_norm_interval["time_end"] = next_norm_interval["time_end"]
                norm_intervals.pop(index + 1)
            else:
                index += 1

        last_interval = norm_intervals[-1]
        if last_interval['time_end'] != timedelta(days=0) and last_interval['time_end'] < timedelta(days=1):
            free_interval = {
                "time_begin": last_interval['time_end'],
                "idle_type__name": free_interval_name,
                "time_end": timedelta(days=0)
            }
            norm_intervals.append(free_interval)
        else:
            free_interval = {
                "time_begin": last_interval['time_end'] - timedelta(days=1),
                "idle_type__name": free_interval_name,
                "time_end": norm_intervals[0]['time_begin']
            }
            norm_intervals.insert(0, free_interval)

        first_interval = norm_intervals[0]
        last_interval = norm_intervals[-1]
        if first_interval["time_begin"] != timedelta(days=0) and last_interval['time_end'] == timedelta(days=0):
            free_interval = {
                "time_begin": timedelta(days=0),
                "idle_type__name": free_interval_name,
                "time_end": first_interval["time_begin"]
            }
            norm_intervals.insert(0, free_interval)
        formatted_norm_intervals = [self.format_norm_intervals(
            interval) for interval in norm_intervals]

        return formatted_norm_intervals

    def format_norm_intervals(self, interval):
        return dict(
            time_begin=int(interval["time_begin"].total_seconds()),
            time_end=int(interval["time_end"].total_seconds()),
            intersection="intersection" in interval.keys(),
            idle_type__name=interval["idle_type"].name if "idle_type" in interval.keys(
            ) else interval["idle_type__name"]
        )

    def get_context_data(self, *args, **kwargs):
        context = super(IdleTypeNormingDictView1,
                        self).get_context_data(*args, **kwargs)

        idle_qs = self.get_norming_queryset(self.PREFIX.idle_type)
        idle_norming = defaultdict(list)
        for norm in idle_qs:
            idle_norming[(norm.idle_type, norm.norming_type)].append(norm)
        # Отбираем необходимые виды простоев с типом нормирования в минутах
        idles_types = [idle_type for idle_type,
                       norming_type in idle_norming if norming_type == TYPES.MINUTES_IN_SHIFT]

        SHAS_intervals = self.get_norm_intervals_for_vehicle_type(
            idles_types, VehicleType.get_shas_type_id())
        PDM_intervals = self.get_norm_intervals_for_vehicle_type(
            idles_types, VehicleType.get_pdm_type_id())
        SBU_intervals = self.get_norm_intervals_for_vehicle_type(
            idles_types, VehicleType.get_sbu_type_id())

        if self.enterprise:
            additional = IdleTypeNorming.additional_relations(self.enterprise)
        else:
            additional = []
        context["additional"] = json.dumps(additional)
        context["SHAS_intervals"] = json.dumps(SHAS_intervals)
        context["PDM_intervals"] = json.dumps(PDM_intervals)
        context["SBU_intervals"] = json.dumps(SBU_intervals)

        return context

    # region 'Интервалы нормирования'

    def get_intervals_norming_queryset(self):
        return IdleTypeIntervals.objects.select_related('idle_type').filter(idle_type__enterprise_id=self.enterprise.id)

    def get_instance_interval_list(self, idle_type_id):
        """
        :param idle_type_id:
        :return: Список интервалов для нормативного простоя
        """
        return self.get_intervals_norming_queryset().filter(idle_type_id=idle_type_id)

    def interval_idle_id_to_list(self, interval_type_qs):
        """
        Возвращает список id интервалов нормирования
        :param interval_type_qs:
        :return:
        """
        return [str(x.id) for x in interval_type_qs]

    def intervals_to_dict(self, interval, **kwargs):
        """
        сериализация нормативов на простои
        """
        norms = list(interval.interval_type.values_list('id', flat=True))
        return dict(
            id=interval.id,
            time_begin=int(interval.time_begin.total_seconds()
                           ) if interval.time_begin is not None else None,
            time_begin_str=str(interval.time_begin),
            time_end=int(interval.time_end.total_seconds()
                         ) if interval.time_end else None,
            time_end_str=str(interval.time_end),
            time_begin_second=int(
                interval.time_begin.total_seconds()) if interval.time_begin else None,
            time_end_second=int(interval.time_end.total_seconds()
                                ) if interval.time_end else None,
            idle_type=interval.idle_type.id,
            t1=interval.time_begin,
            t2=interval.time_end,
            normings=norms[0] if len(norms) == 1 else norms,
            **kwargs
        )

    # endregion


class IdleReportViewParams(HHMMSSMixin):
    controls = {
        "vehicle__enterprise": EnterprisesControlRB,
        "vehicle": VehiclesControlRB,
        "time_from_to": DateTimeRangeUberControlRB,
        "time": IdleDurationControl,
        "time_max": IdleMaxDurationControlV2,
    }

    datafields = [
        # hidden

        {'name': 'id', 'type': 'int', "report": False, 'text': 'ID'},
        {'name': 'vehicle', 'type': 'int', 'report': False},
        {'name': 'idle_type', 'type': 'string', 'report': False},
        {'name': 'closed', 'type': 'bool', "report": False},
        {"name": "reason", "type": "int"},
        {"name": "supervisor_category", "type": "int"},
        {'name': 'automatic_reason_name', 'type': 'text', "report": False},
        {'name': 'driver_reason_name', 'type': 'text', "report": False},
        {'name': 'duration', 'type': 'text', "report": False},
        {'name': 'manual_created', 'type': 'int'},

        # For enterprises with edit_idles_only_with_inoperable_asd setting
        {'name': 'time_start_editable', 'type': 'bool',
            'calculate_method': 'is_time_start_editable', 'report': False},
        {'name': 'time_stop_editable', 'type': 'bool',
            'calculate_method': 'is_time_stop_editable', 'report': False},

        # visible
        {"name": "vehicle_name", "type": "string",
            "text": __("Мобильный объект")},
        {"name": "driver", "type": "string", "text": "Водитель",
            "calculate_method": "get_driver"},
        {"name": "vehicle_kind", "type": "string", "calculate_method": "get_vehicle_kind",
            "report": False, "text": __("Вид техники")},
        {"name": "time_start", "type": "date", "text": __("Начало простоя")},
        {"name": "time_stop", "type": "date", "text": __("Конец простоя")},
        {"name": "c_duration", "type": "string", "text": __(
            "Продолжительность"), "calculate_method": "convert_duration"},
        {"name": "reason_name", "type": "string", "text": __("Тип")},
        {"name": "reason_code", "type": "string",
            "calculate_method": "calculate_reason_code", "text": "Источник типа простоя"},
        {'name': 'manual', 'text': "Источник простоя",
            'type': 'string', "calculate_method": "get_manual"},
        {'name': 'split_cell', 'text': __(
            'Разделить простой'), "calculate_method": None},
        {"name": "supervisor_comment", "type": "string",
            "text": __("Описание причины")},
        # {'name': 'geometries', 'text': __('Геозоны'), "calculate_method": "calculate_geometries"},
        # {"name": 'vehicle_part_name', "type": "string", "text": __("Запчасть")},
        # {'name': 'map_cell', 'text': __('Показать на карте'), "calculate_method": None},
    ]

    columns = [
        {'datafield': 'id', 'hidden': True},
        {'datafield': 'idle_type', 'hidden': True},
        {'datafield': 'supervisor_category', 'hidden': True},
        {'datafield': 'closed', 'hidden': True},
        {"datafield": "reason", "hidden": True},
        {"datafield": "automatic_reason_name", "hidden": True},
        {"datafield": "driver_reason_name", "hidden": True},
        {"datafield": "manual_created", "hidden": True},
        {"datafield": "duration", "hidden": True},
        {"datafield": "vehicle", "hidden": True},
        {"datafield": "time_start_editable", "hidden": True},
        {"datafield": "time_stop_editable", "hidden": True},

    ]
    if not SHOW_VEHICLE_PARTS:
        columns.append({"datafield": "vehicle_part_name", "hidden": True})

    def convert_duration(self, raw):
        return self.ddhhmmss(raw['duration'], '0')

    def calculate_reason_code(self, raw):
        if ((raw['supervisor_reason'] and raw['driver_reason'])
                or (raw['supervisor_reason'] and raw['automatic_reason'])):
            return "Диспетчер(переопределено)"
        if raw['supervisor_reason']:
            return "Диспетчер"
        if raw['driver_reason']:
            return "Водитель"
        if raw['automatic_reason']:
            return "Авто"
        return "Не опр."

    def _worked(self, veh_id, _time):  # type: (IdleReportViewParams, int, datetime.datetime) -> bool
        return True

    # type: (IdleReportViewParams, dict) -> bool
    def is_time_start_editable(self, raw):
        if not self.edit_idles_only_with_inoperable_asd:
            return True
        return not self._worked(raw["vehicle"], raw["time_start"])

    # type: (IdleReportViewParams, dict) -> bool
    def is_time_stop_editable(self, raw):
        if not self.edit_idles_only_with_inoperable_asd:
            return True
        return not self._worked(raw["vehicle"], raw["time_stop"])

    def get_manual(self, raw):
        return "Ручной" if raw['manual_created'] else "АСУГР"

    @cached_property
    # type: (IdleReportViewParams) -> bool
    def edit_idles_only_with_inoperable_asd(self):
        if not PIT_IMPORTED:
            return False
        return get_enterprise_setting_value(
            self.filters_data["vehicle__enterprise"], 'edit_idles_only_with_inoperable_asd'
        ) or False


class CheckTimesMixin:
    def __init__(self, *args, **kwargs):
        super(CheckTimesMixin, self).__init__(*args, **kwargs)

    def check_times(self, time_start, time_stop, closed, vehicle=None):
        if self.instance.id:
            vehicle = self.instance.vehicle

        if not (time_start and time_stop and vehicle):
            return

        # Вариант с честными простоями
        checked_time_begin = time_start
        checked_time_end = time_stop
        if not closed:
            checked_time_end = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        flt_q = (
            Q(time_start__lt=checked_time_end, time_stop__gt=checked_time_begin)
        )
        idle_qs = Idle.objects.filter(flt_q, vehicle=vehicle).exclude(
            id=self.instance.id).order_by('-time_stop')

        if idle_qs.exists():
            cross_idle = idle_qs.first()
            tz = pytz.timezone(vehicle.enterprise.time_zone)
            local_time_start = cross_idle.time_start.astimezone(
                tz).strftime("%d.%m.%Y %H:%M:%S")
            local_time_stop = cross_idle.time_stop.astimezone(
                tz).strftime("%d.%m.%Y %H:%M:%S")
            if cross_idle.closed:
                error_message = _("Простой не может пересекаться с существующим!<br/><br/>Пересекает простой:<br/>с ") + \
                    local_time_start + _(" по ") + local_time_stop
            else:
                error_message = _(
                    "Простой не может пересекаться с существующим!<br/><br/>Пересекает неоконченный простой:<br/>с началом в ") + local_time_start
            raise ValidationError(_(error_message), code='invalid')


class IdleManualFormMixin(CheckTimesMixin):
    def __init__(self, shift_range, *args, **kwargs):
        super(IdleManualFormMixin, self).__init__(*args, **kwargs)
        self.shift_begin, self.shift_end = shift_range
        self.fields['supervisor_reason'].required = True


class InoperabilityFormValidationMixin:
    def __init__(self, wrd_id, _date, edit_idles_only_with_inoperable_asd, *args, **kwargs):
        # type: (InoperabilityFormValidationMixin, tuple, int, datetime.datetime, bool, *Any, **Any) -> None
        super(InoperabilityFormValidationMixin, self).__init__(*args, **kwargs)
        self.edit_idles_only_with_inoperable_asd = edit_idles_only_with_inoperable_asd
        self.wrd_id = wrd_id
        self.date = _date

    # type: (InoperabilityFormValidationMixin, int, datetime.datetime) -> bool
    def _worked(self, veh_id, _time):
        return True

    # type: (InoperabilityFormValidationMixin) -> datetime.datetime
    def clean_time_start(self):
        time_start = self.cleaned_data["time_start"]
        if not self.edit_idles_only_with_inoperable_asd:
            return time_start
        if not self._worked(self.instance.vehicle_id, time_start):
            return time_start
        raise ValidationError(
            _("Время начала простоя может редактироваться только в интервале неработоспособности АСД")
        )

    # type: (InoperabilityFormValidationMixin) -> datetime.datetime
    def clean_time_stop(self):
        time_stop = self.cleaned_data["time_stop"]
        if not self.edit_idles_only_with_inoperable_asd:
            return time_stop
        if not self._worked(self.instance.vehicle_id, time_stop):
            return time_stop
        raise ValidationError(
            _("Время конца простоя может редактироваться только в интервале неработоспособности АСД")
        )


class IdleCreateForm(IdleManualFormMixin, forms.ModelForm):
    class Meta(object):
        model = Idle
        fields = ['vehicle', 'time_start', 'time_stop', 'closed',
                  'supervisor_reason', 'supervisor_category', 'supervisor_comment', 'vehicle_part']

    def clean(self):
        cleaned_data = super(IdleCreateForm, self).clean()
        time_start = self.cleaned_data.get('time_start')
        time_stop = self.cleaned_data.get('time_stop')
        vehicle = self.cleaned_data.get('vehicle')
        closed = self.cleaned_data.get('closed')
        if time_start and time_stop and vehicle:
            self.check_times(time_start, time_stop, closed, vehicle=vehicle)
        return cleaned_data

    def save(self, commit=True):
        idle_obj = super(IdleCreateForm, self).save(commit=False)
        if idle_obj.supervisor_reason is not None:
            idle_obj.supervisor_reason_change = timezone.now()
            idle_obj.supervisor_reason_first_change = idle_obj.supervisor_reason_change
        idle_obj.manual_created = True
        idle_obj.lon = 0
        idle_obj.lat = 0
        if commit:
            idle_obj.save()
        return idle_obj


class IdleAutoUpdateForm(InoperabilityFormValidationMixin, CheckTimesMixin, forms.ModelForm):
    class Meta(object):
        model = Idle
        fields = ['time_start', 'time_stop', 'closed', 'supervisor_reason',
                  'supervisor_category', 'supervisor_comment', 'vehicle_part']

    def clean(self):
        cleaned_data = super(IdleAutoUpdateForm, self).clean()
        time_start = self.cleaned_data.get('time_start')
        time_stop = self.cleaned_data.get('time_stop')
        closed = self.cleaned_data.get('closed')
        if time_start and time_stop:
            self.check_times(time_start, time_stop, closed)
        return cleaned_data


class IdleManualUpdateForm(IdleManualFormMixin, InoperabilityFormValidationMixin, forms.ModelForm):
    class Meta(object):
        model = Idle
        fields = ['time_start', 'time_stop', 'closed', 'supervisor_reason', 'supervisor_category',
                  'supervisor_comment', 'vehicle_part']

    def clean(self):
        cleaned_data = super(IdleManualUpdateForm, self).clean()
        time_start = self.cleaned_data.get('time_start')
        time_stop = self.cleaned_data.get('time_stop')
        closed = self.cleaned_data.get('closed')
        if time_start and time_stop:
            self.check_times(time_start, time_stop, closed)
        return cleaned_data


class IdleSplitForm(forms.ModelForm):
    split_reason = forms.ModelChoiceField(
        queryset=IdleType.objects.none(), label=__('Причина простоя'))
    split_supervisor_category = forms.ModelChoiceField(queryset=IdleTypeTree.objects.none(
    ), label=__('Дерево классификации видов простоев'), required=False)
    split_comment = forms.CharField(
        max_length=16384, required=False, label=__('Описание причины'))

    class Meta(object):
        model = Idle
        fields = ['time_start', 'supervisor_reason',
                  'supervisor_category', 'supervisor_comment', 'vehicle_part']

    def __init__(self, *args, **kwargs):
        super(IdleSplitForm, self).__init__(*args, **kwargs)
        self.fields['time_start'].label = _("Время деления")
        if self.instance:
            enterprise = self.instance.vehicle.enterprise
            self.fields['split_reason'].queryset = IdleType.for_enterprise(
                enterprise)
            self.fields['split_supervisor_category'].queryset = IdleTypeTree.for_enterprise(
                enterprise)
        from copy import deepcopy
        self.split_idle = deepcopy(kwargs['instance'])
        self.split_idle.pk = None

    def clean_time_start(self):
        time_start = self.cleaned_data['time_start']
        if time_start <= self.instance.time_start:
            raise ValidationError(
                _("Время деления не может быть меньше времени начала простоя."))
        if self.instance.closed and time_start >= self.instance.time_stop:
            raise ValidationError(
                _("Время деления не может быть больше времени окончания простоя."))
        if not self.instance.closed and time_start > datetime.datetime.utcnow().replace(tzinfo=pytz.UTC):
            raise ValidationError(
                _("Для неоконченного простоя время деления не может быть больше текущего."))
        return time_start

    def save(self, commit=True):
        time_split = self.cleaned_data['time_start']
        self.split_idle.time_stop = time_split
        self.split_idle.supervisor_reason = self.cleaned_data['split_reason']
        self.split_idle.supervisor_category = self.cleaned_data['split_supervisor_category']
        self.split_idle.supervisor_comment = self.cleaned_data['split_comment']
        if not self.split_idle.manual_created:
            self.split_idle.closed = True

        if self.split_idle.time_stop > self.instance.time_start:
            self.instance.time_start = self.split_idle.time_stop

        idle = super(IdleSplitForm, self).save(commit)
        if commit:
            self.split_idle.save()
        return [self.split_idle, idle]


class IdleReportViewActions(object):
    get_actions = ReportView.get_actions + ["goto_map"]
    post_actions = ReportView.post_actions + ["save_idle",
                                              "create_idle",
                                              "delete_idle",
                                              "split_idle",
                                              "split_idle_with_eto",
                                              "merge_idle",
                                              "edit_idle",
                                              "delete_idle_with_recalc"]
    create_form_class = IdleCreateForm
    update_auto_form_class = IdleAutoUpdateForm
    update_manual_form_class = IdleManualUpdateForm
    NONE_REASON = _("Без причины")

    dashboard_actions = {
        1: __("Изменена причина простоя"),
        2: __("Удален простой"),
        3: __("Добавлен простой"),
        4: __("Разделен простой"),
        5: __("Объединены простои")
    }

    def get_idle_id(self, request_dict):
        return request_dict.get('id')

    def error_response(self, error):
        return self.j(dict(error=error))

    def goto_map(self, request):
        idle_id = request.GET.get('record_id')
        try:
            idle = Idle.objects.get(id=idle_id)
        except (KeyError, Idle.DoesNotExist):
            raise Http404
        return HttpResponseRedirect(reverse("report_simple_map") + "?lon=%s&lat=%s" % (idle.lon, idle.lat))

    def _add_idle_log(self, old_idle, idle, edit_idle=None):
        reason_name_change = ''
        reason_category_change = ''
        idle_comment_change = ''
        comma = ''
        old_reason = old_idle.reason
        new_reason = idle.reason

        old_name = new_name = None
        if old_reason and old_reason.name:
            old_name = old_reason.name
        if new_reason and new_reason.name:
            new_name = new_reason.name
        if (new_name is not None) and old_name != new_name:
            reason_name_change = _(' причина простоя "{old}" '
                                   ' => "{new}"').format(
                old=old_name or '-',
                new=new_name,
            )

        old_name = new_name = None
        if old_reason:
            old_name = (
                old_reason.organization_category.name if old_reason.organization_category else None)
        if new_reason:
            new_name = (
                new_reason.organization_category.name if new_reason.organization_category else None)

        if all([old_name is not None, new_name is not None]) and old_name != new_name:
            if reason_name_change != '':
                comma = ','
            reason_category_change = _(
                '{comma} категория простоя: "{old}" => "{new}"'
            ).format(
                comma=comma,
                old=old_name,
                new=new_name,
            )
        if old_idle.supervisor_comment != idle.supervisor_comment:
            if reason_name_change != '' or reason_category_change != '':
                comma = ','
            idle_comment_change = _(
                '{comma} комментарий простоя: "{old}" => "{new}"'
            ).format(
                comma=comma,
                old=old_idle.supervisor_comment,
                new=idle.supervisor_comment
            )
        self.dashboard(
            idle.vehicle.enterprise,
            1,
            _("Изменение простоя ({manual_created}) МО {mo} от {begin}:"
              "{name_change}{category_change}{comment_change}")
            .format(mo=idle.vehicle.name,
                    name_change=reason_name_change,
                    category_change=reason_category_change,
                    comment_change=idle_comment_change,
                    begin=timezone.localtime(
                        idle.time_start).strftime(TIME_FORMAT),
                    manual_created=_('ручной ввод')
                    if idle.manual_created or edit_idle
                    else _('автоматически')),
            'Idle',
            idle.id,
            original_object=old_idle,
            final_object=idle,
        )

    def save_idle(self, request):
        idle_id = self.get_idle_id(request.POST)
        idle = Idle.objects \
            .select_related("vehicle") \
            .filter(id=idle_id).first() if idle_id else None
        if idle:
            original_object = copy.deepcopy(idle)
            edit_idles_only_with_inoperable_asd = False
            if PIT_IMPORTED:
                edit_idles_only_with_inoperable_asd = get_enterprise_setting_value(
                    idle.vehicle.enterprise_id, 'edit_idles_only_with_inoperable_asd'
                ) or False

            if idle.manual_created:
                form = self.update_manual_form_class(
                    self.shift_range,
                    self.filters_data.shift,
                    self.filters_data.shift_date,
                    edit_idles_only_with_inoperable_asd,
                    request.POST,
                    instance=idle)
            else:
                form = self.update_auto_form_class(
                    self.filters_data.shift,
                    self.filters_data.shift_date,
                    edit_idles_only_with_inoperable_asd,
                    request.POST,
                    instance=idle)

            old_reason = idle.reason.name if idle.reason else self.NONE_REASON
            if form.is_valid():
                if form.instance.supervisor_reason \
                        and form.instance.supervisor_reason.name != old_reason:
                    form.instance.supervisor_reason_change = timezone.now()
                    if form.instance.supervisor_reason_first_change is None:
                        form.instance.supervisor_reason_first_change = \
                            form.instance.supervisor_reason_change
                elif not form.instance.supervisor_reason:
                    form.instance.supervisor_reason = None

                idle = form.save()

                self._add_idle_log(original_object, idle)

                return self.j(dict(idle=self.serialize_obj(
                    idle,
                    self._date_report_serializer
                )))
            else:
                resp = dict(
                    non_field_errors=", ".join(form.non_field_errors()),
                    errors=[(force_text(field.label),
                             ", ".join(field.errors)) for field in form
                            if field.errors],
                )
                return self.j(resp)
        return self.error_response(_('Запись не найдена'))

    def delete_idle(self, request):
        idle_id = self.get_idle_id(request.POST)
        idle = Idle.objects.filter(id=idle_id).first() if idle_id else None
        if idle:
            idle.delete()
            reason = idle.reason.name if idle.reason else self.NONE_REASON
            self.dashboard(idle.vehicle.enterprise, 2,
                           _("Удален простой ({manual_created})  МО {mo} от {begin}. "
                             "Причина простоя '{reason}'")
                           .format(mo=idle.vehicle.name,
                                   begin=timezone.localtime(
                                       idle.time_start).strftime(TIME_FORMAT),
                                   reason=reason,
                                   manual_created=_('ручной ввод')
                                   if idle.manual_created
                                   else _('автоматически')),
                           'Idle',
                           idle.id,
                           original_object=idle,
                           )
            return self.j(dict(idle=self.serialize_obj(idle, self._date_report_serializer)))
        else:
            return self.error_response(_('Запись не найдена'))

    def delete_idle_with_recalc(self, request):
        idle_id = self.get_idle_id(request.POST)
        idle = Idle.objects.filter(id=idle_id).first() if idle_id else None
        if idle:
            from_date = idle.time_start
            to_date = idle.time_stop

            def callback_func(current):
                print(current)

            recalculator = StoppageNurse(
                from_date=from_date,
                to_date=to_date,
                vehicle_id=idle.vehicle_id,
                callback_func=callback_func
            )

            recalculator.recalc_stoppage()
            recalculator.recalc_rawstoppage()

            reason = idle.reason.name if idle.reason else self.NONE_REASON
            self.dashboard(idle.vehicle.enterprise, 2,
                           _("Удален простой ({manual_created})  МО {mo} от {begin}. "
                             "Причина простоя '{reason}'")
                           .format(mo=idle.vehicle.name,
                                   begin=timezone.localtime(
                                       idle.time_start).strftime(TIME_FORMAT),
                                   reason=reason,
                                   manual_created=_('ручной ввод')
                                   if idle.manual_created
                                   else _('автоматически')),
                           'Idle',
                           idle.id,
                           original_object=idle,
                           )
            return self.j(dict(idle=self.serialize_obj(idle, self._date_report_serializer)))
        else:
            return self.error_response(_('Запись не найдена'))

    @transaction.atomic
    def create_idle(self, request):
        form = self.create_form_class(self.shift_range, request.POST)
        if form.is_valid():
            idle = form.save()
            self.dashboard(
                idle.vehicle.enterprise, 3,
                _("Добавлен простой ({manual_created}) МО {mo}.\n Начало простоя {begin}. \n"
                  "Окончание простоя {end}.\n Причина простоя {reason}\nПростой завершён {closed}").format(
                    mo=idle.vehicle.name,
                    begin=timezone.localtime(
                        idle.time_start).strftime(TIME_FORMAT),
                    end=timezone.localtime(idle.time_stop).strftime(
                        TIME_FORMAT) if idle.time_stop else _("Не задано"),
                    closed="Да" if idle.closed else "Нет",
                    reason=idle.reason.name if idle.reason else self.NONE_REASON,
                    manual_created=_('ручной ввод') if idle.manual_created else _(
                        'автоматически')
                ),
                'Idle',
                idle.id,
                final_object=idle,
            )
            return self.j(self.serialize_obj(idle, self._date_report_serializer))
        resp = dict(
            non_field_errors=", ".join(form.non_field_errors()),
            errors=[(force_text(field.label), ", ".join(field.errors))
                    for field in form if field.errors],
        )
        return self.j(resp)

    def split_idle(self, request):
        idle_id = self.get_idle_id(request.POST)
        idle = Idle.objects.filter(id=idle_id).first() if idle_id else None
        if not idle:
            resp = dict(non_field_errors=_('Запись не найдена'))
        else:
            begin_time = timezone.localtime(idle.time_start)
            form = IdleSplitForm(request.POST, instance=idle)
            if form.is_valid():
                idles = form.save()
                self.dashboard(idle.vehicle.enterprise, 4,
                               _("Разделен простой ({manual_created}) МО {mo} от {begin}. "
                                 "Получены простои: \n"
                                 "'{reason0}' {begin0} - {end0}, \n'{reason1}' {begin1} - {end1}\n").format(
                                   mo=idle.vehicle.name,
                                   begin=begin_time.strftime(TIME_FORMAT),
                                   reason0=idles[0].reason.name if idles[0].reason else self.NONE_REASON,
                                   reason1=idles[1].reason.name if idles[1].reason else self.NONE_REASON,
                                   begin0=timezone.localtime(
                                       idles[0].time_start).strftime(TIME_FORMAT),
                                   begin1=timezone.localtime(
                                       idles[1].time_start).strftime(TIME_FORMAT),
                                   end0=timezone.localtime(idles[0].time_stop).strftime(
                                       TIME_FORMAT) if idles[0].time_stop else _("Не задано"),
                                   end1=timezone.localtime(idles[1].time_stop).strftime(
                                       TIME_FORMAT) if idles[1].time_stop else _("Не задано"),
                                   manual_created=_('ручной ввод') if idle.manual_created
                                   else _('автоматически')
                               ),
                               'Idle',
                               idle.id,
                               original_object=idles[0],
                               final_object=idles[1],
                               )
                resp = dict(idles=[self.serialize_obj(
                    idle, self._date_report_serializer) for idle in idles])
            else:
                resp = dict(
                    non_field_errors=", ".join(form.non_field_errors()),
                    errors=[(force_text(field.label), ", ".join(field.errors))
                            for field in form if field.errors],
                )
        return self.j(resp)

    def split_idle_with_eto(self, request):
        idle_id = request.POST.get('idle_id')
        idle: Idle = (
            Idle.objects.filter(id=idle_id).first()
            if idle_id
            else None
        )
        if not idle:
            resp = dict(non_field_errors=_('Запись не найдена'))
        else:

            idle_splitter = IdleSplitter(idle)
            new_idles = idle_splitter.split()

            idles_string = '\n'.join(
                ['{reason} {begin} - {end}.'.format(
                    reason=idle.reason.name if idle.reason else self.NONE_REASON,
                    begin=timezone.localtime(
                        idle.time_start).strftime(TIME_FORMAT),
                    end=timezone.localtime(
                        idle.time_stop).strftime(TIME_FORMAT)
                )
                    for idle in new_idles]
            )

            self.dashboard(idle.vehicle.enterprise, 4,
                           ("Разделен простой по нормам МО {mo} от {begin} до {end}.\n"
                            "Получены простои: \n"
                            "'{idles_string}'\n").format(
                               mo=idle.vehicle.name,
                               begin=timezone.localtime(idle.time_start),
                               end=timezone.localtime(idle.time_stop),
                               idles_string=idles_string
                           ),
                           'Idle',
                           )
            resp = dict(idles=[self.serialize_obj(
                idle, self._date_report_serializer) for idle in new_idles])

        return self.j(resp)

    def merge_idle(self, request):
        try:
            id_1, id_2 = request.POST['idle_1'], request.POST['idle_2']
            idle_1 = Idle.objects.get(id=id_1)
            idle_2 = Idle.objects.get(id=id_2)
        except (KeyError, Idle.DoesNotExist, ObjectDoesNotExist):
            resp = dict(non_field_errors=_("Записи не найдены"))
            return self.j(resp)
        first_end = min(idle_1.time_stop, idle_2.time_stop)
        second_start = max(idle_1.time_start, idle_2.time_start)
        interval = get_core_enterprise_settings(
            idle_1.vehicle.enterprise_id).idle_merging_interval
        fmt_interval = gmtime(interval)
        if idle_1.vehicle_id != idle_2.vehicle_id:
            resp = dict(non_field_errors=_(
                "Выбраны простои разных мобильных объектов"))
            return self.j(resp)
        elif interval < (second_start - first_end).total_seconds():
            resp = dict(non_field_errors=_("Объединить можно только два простоя, интервал между которыми не больше %(hour)s часов %(min)s минут %(sec)s секунд") % {
                        'hour': fmt_interval.tm_hour, 'min': fmt_interval.tm_min, 'sec': fmt_interval.tm_sec})
            return self.j(resp)
        else:
            idles_to_delete = Idle.objects.filter(time_start__gt=first_end, time_start__lt=second_start,
                                                  vehicle_id=idle_1.vehicle_id)
            if idles_to_delete:
                for i in idles_to_delete:
                    i.delete()
            idle, merge_idle = (
                idle_1, idle_2) if idle_1.time_start > idle_2.time_start else (idle_2, idle_1)
            original_merge_idle = copy.deepcopy(merge_idle)
            merge_idle.time_stop = idle.time_stop
            merge_idle.save()
            idle.delete()
            self.dashboard(idle.vehicle.enterprise, 5,
                           _("Объединены простои ({manual_created}) МО {mo}. \n"
                             "'{reason0}' {begin0} - {end0},\n"
                             "'{reason1}' {begin1} - {end1}.\n"
                             "Получен простой: '{reason}' {begin} - {end}"
                             ).format(
                               mo=idle.vehicle.name,
                               reason0=idle_1.reason.name if idle_1.reason else self.NONE_REASON,
                               reason1=idle_2.reason.name if idle_2.reason else self.NONE_REASON,
                               begin0=timezone.localtime(
                                   idle_1.time_start).strftime(TIME_FORMAT),
                               begin1=timezone.localtime(
                                   idle_2.time_start).strftime(TIME_FORMAT),
                               end0=timezone.localtime(idle_1.time_stop).strftime(
                                   TIME_FORMAT) if idle_1.time_stop else _("Не задано"),
                               end1=timezone.localtime(idle_2.time_stop).strftime(
                                   TIME_FORMAT) if idle_2.time_stop else _("Не задано"),
                               reason=merge_idle.reason.name if merge_idle.reason else self.NONE_REASON,
                               begin=timezone.localtime(
                                   merge_idle.time_start).strftime(TIME_FORMAT),
                               end=timezone.localtime(merge_idle.time_stop).strftime(
                                   TIME_FORMAT) if merge_idle.time_stop else _("Не задано"),
                               manual_created=_('ручной ввод') if idle.manual_created
                               else _('автоматически')
                           ),
                           'Idle',
                           idle.id,
                           original_object=original_merge_idle,
                           final_object=merge_idle,
                           )
            resp = dict(idle=self.serialize_obj(
                merge_idle, self._date_report_serializer))
        return self.j(resp)

    def edit_idle(self, request):
        idles = json.loads(request.POST.get('data', None))
        idle_ids = idles.get('idle_ids', None)
        category = idles.get('category', None)
        reason = idles.get('reason', None)
        comment = idles.get('supervisor_comment', None)
        resps = list()

        for idle_id in idle_ids:
            idle = Idle.objects.filter(id=idle_id).first() \
                if Idle.objects.filter(id=idle_id).exists() else None
            if idle:
                old_idle = copy.deepcopy(idle)
                idle.supervisor_category = IdleTypeTree.objects.filter(
                    id=category).first()
                idle.supervisor_reason = IdleType.objects.filter(
                    id=reason).first()
                idle.supervisor_comment = comment
                idle.save()
                self._add_idle_log(old_idle, idle, True)
                resp = dict(idle=self.serialize_obj(
                    idle,
                    self._date_report_serializer
                ))
            else:
                resp = dict(non_field_errors=_("Записи не найдены"))

            resps.append(resp)
        return self.j(resps)


class IdleReasonUpdateForm(forms.ModelForm):
    class Meta(object):
        model = Idle
        fields = ['supervisor_reason', 'supervisor_comment']

    def __init__(self, *args, **kwargs):
        super(IdleReasonUpdateForm, self).__init__(*args, **kwargs)
        self.fields['supervisor_reason'].required = True


class IdleReasonEditView(AngularRestView):
    name = "core_idle_reason_edit_view"
    update_form_class = IdleReasonUpdateForm

    def can_delete(self, deleted_object):
        return False

    def get_queryset(self):
        return Idle.objects.filter(vehicle__enterprise__id__in=self.logic_user.enterprises_ids)


class IdleReportView(IdleReportViewParams, IdleReportViewActions, ReportView):
    name = "core_idle_report_view"
    menu_name = __("Форма простоев core")
    verbose_name = __("Форма простоев core")
    description = __("Форма для работы с простоями")
    template_name = "idle_report.jinja"
    usersort = False

    @property
    def show_endless(self):
        return self.data.get("show_endless") == "true"

    @property
    def only_cross(self):
        return self.data.get("only_cross") == "true"

    @property
    def technical(self):
        return self.data.get("technical") == "true"

    @property
    def controls_params(self):
        return {
            "time": {
                "endless": self.show_endless,
            }
        }

    @property
    def shift_range(self):
        shift_range = self.filters_data.time_from_to[0:2]
        # shift = WorkRegimeDetail.objects.get(id=self.filters_data.shift)
        # shift_range = shift.get_borders(self.filters_data.shift_date)
        return shift_range

    @cached_property
    def vehicles_dict(self):
        vehicles_iter = six.moves.filter(
            lambda obj: obj.id in self.filters_data.vehicle,
            self.logic_user.repo_vehicles
        )
        return {obj.id: obj for obj in vehicles_iter}

    def _date_report_serializer(self, data):
        """ Переопределяет метод родительского класса,
        чтобы не было проблем с отображением даты.

        :param data:
        :return:
        """
        return data

    def get_vehicle_kind(self, raw):
        vehicle = self.vehicles_dict.get(raw['vehicle'])
        vehicle_kind_codes = vehicle.vehicle_kind_codes if vehicle else None
        if vehicle_kind_codes:
            vehicle_kind = (force_text(VEHICLE_KINDS.get(code).name)
                            for code in vehicle_kind_codes)
            return ', '.join(vehicle_kind)
        else:
            return '-'

    @cached_property
    def person_by_vehicle_id_and_date_and_shift_id(self) -> Dict[Tuple[int, datetime.date, int], Person]:
        start_date = self.shift_range[0].date()
        end_date = self.shift_range[1].date()
        sdo_plans = list(
            PlanMessage.objects.filter(
                date__gte=start_date, date__lte=end_date)
            .annotate(vehicle_id=F('truck_id'))
            .values('shift_id', 'date', 'vehicle_id', 'operator_id')
        )
        sbu_plans = list(
            PlanMessageSBU.objects.filter(
                date__gte=start_date, date__lte=end_date)
            .annotate(vehicle_id=F('drill_id'))
            .values('shift_id', 'date', 'vehicle_id', 'operator_id')
        )
        operator_by_key = {
            (plan['vehicle_id'], plan['date'], plan['shift_id']): plan['operator_id']
            for plan in sdo_plans + sbu_plans
        }
        operators_ids = set(operator_by_key.values())
        employees = Employee.objects.filter(
            id__in=operators_ids).select_related('person')
        person_by_operator_id = {
            employee.id: employee.person for employee in employees}
        operator_by_key = {
            vehicle_id_and_date_and_shift_id: person_by_operator_id.get(
                operator_id)
            for vehicle_id_and_date_and_shift_id, operator_id in operator_by_key.items()
        }
        return operator_by_key

    @cached_property
    def enterprise_shifts(self) -> List:
        return list(WorkRegimeDetail.objects.filter(work_regime__enterprise_id=self.filters_data.vehicle__enterprise))

    @cached_property
    def regime_list(self) -> List[Tuple[datetime.datetime, datetime.datetime, WorkRegimeDetail]]:
        time_from, time_to = self.shift_range
        vehicle = Vehicle.objects.filter(
            id__in=self.filters_data.vehicle).first()
        enterprise = vehicle.enterprise if vehicle else Enterprise.objects.get(
            id=ENTERPRISE_ID)
        current_regime = enterprise.accounting_shift
        regime_list = get_regime_list_for_time_range(
            time_from, time_to, current_regime)
        return regime_list

    def get_shift_date_and_shift_id(
        self, time: datetime.datetime, time_like: str = 'start'
    ) -> Tuple[Union[datetime.date, None], Union[int, None]]:
        assert time_like in ['start', 'stop']
        for shift_start, shift_end, shift, shift_date in self.regime_list:
            if time_like == 'start' and shift_start <= time < shift_end:
                return shift_date, shift.id
            elif time_like == 'stop' and shift_start < time <= shift_end:
                return shift_date, shift.id
        return None, None

    def get_driver(self, row: Dict) -> str:
        start_shift_date, start_shift_id = self.get_shift_date_and_shift_id(
            row['time_start'], time_like='start')
        start_person = self.person_by_vehicle_id_and_date_and_shift_id.get(
            (row['vehicle'], start_shift_date, start_shift_id)
        )

        end_shift_date, end_shift_id = self.get_shift_date_and_shift_id(
            row['time_stop'], time_like='stop')
        stop_person = self.person_by_vehicle_id_and_date_and_shift_id.get(
            (row['vehicle'], end_shift_date, end_shift_id)
        )
        start_person_name = start_person.get_short_name() if start_person else '---'
        stop_person_name = stop_person.get_short_name() if stop_person else '---'
        if start_person_name != stop_person_name:
            start_person_name = start_person.get_short_name() if start_person else '---'
            return f'{start_person_name}/{stop_person_name}'
        return start_person_name

    def get_idle_type_tree(self):
        user = UserProfile.objects.get(id=self.logic_user.id)
        IdleTypeTreeLogic = ITTreeLogic(self.filters_data.vehicle__enterprise)
        idle_type_tree = IdleTypeTreeLogic.get_tree(user=user)
        if not idle_type_tree:
            return []
        return idle_type_tree

    def get_technical(self):
        from vist_underground.logic.vehicle_efficiency.idle_calculations import IdleStatsCalculation
        technical_idles_ids = IdleStatsCalculation.get_technical_reason_ids()

        reason_q_arg = (Q(supervisor_reason_id__in=technical_idles_ids)
                        | (Q(supervisor_reason_id__isnull=True) & Q(driver_reason_id__in=technical_idles_ids))
                        | (Q(supervisor_reason_id__isnull=True) & Q(driver_reason_id__isnull=True) & Q(automatic_reason_id__in=technical_idles_ids)))

        return reason_q_arg

    def get_cross_query(self):
        # Принудительно зануляем для пересечек
        minimum_duration_in_sec = int(self.filters_data.time) * 60
        maximum_duration_in_sec = int(self.filters_data.time_max) * 60
        vehicle_ids = self.filters_data.vehicle
        vehicles = Vehicle.objects.filter(id__in=vehicle_ids)

        query = Q(
            time_start__lt=self.shift_range[1], time_stop__gt=self.shift_range[0])

        if minimum_duration_in_sec:
            query &= Q(duration_sec__gte=minimum_duration_in_sec)

        if maximum_duration_in_sec:
            query &= Q(duration_sec__lte=maximum_duration_in_sec)

        cross_idles_ids = []
        duration_sec_rawsql = """extract(second from time_stop-time_start) 
            + extract(minute from time_stop-time_start)*60 
            + extract(hour from time_stop-time_start)*60*60
            + extract(day from time_stop-time_start)*60*60*24"""
        for vehicle in vehicles:
            vehicle_query = query & Q(vehicle_id=vehicle.id)
            idles = list(
                Idle.objects.annotate(
                    duration_sec=RawSQL(duration_sec_rawsql, [])
                )
                .filter(vehicle_query)
                .order_by('time_start')
            )
            if len(idles) < 2:
                continue

            first_idle: Idle = idles[0]
            for current_idle in idles[1:]:
                second_idle: Idle = current_idle

                if first_idle.time_stop > second_idle.time_start:
                    cross_idles_ids += [first_idle.id, second_idle.id]

                if first_idle.time_stop <= second_idle.time_stop:
                    first_idle = second_idle

        return Q(id__in=cross_idles_ids)

    def get_queryset(self, request):
        if USE_TRUE_IDLES:
            q_arg = (
                Q(time_start__lt=self.shift_range[1],
                  time_stop__gt=self.shift_range[0])
            )
            if self.show_endless:
                q_arg |= Q(closed=False, time_start__lt=self.shift_range[1])
        else:
            q_arg = (
                Q(time_start__gte=self.shift_range[0], time_start__lt=self.shift_range[1]) |
                Q(time_stop__gte=self.shift_range[0], time_stop__lt=self.shift_range[1]) |
                Q(time_start__lt=self.shift_range[0],
                  time_stop__gt=self.shift_range[1])
            )
            if self.show_endless:
                q_arg |= Q(closed=False)
        if self.only_cross:
            q_arg &= self.get_cross_query()

        if self.technical:
            q_arg &= self.get_technical()

        idle_qs = Idle.objects.filter(
            q_arg
        ).select_related(
            "vehicle", "automatic_reason", "driver_reason", "supervisor_reason", "vehicle_part"
        ).order_by(
            "-time_stop")

        # idle_type_tree = self.get_idle_type_tree()
        # idle_type_ids = set([it["idle_type"] for it in idle_type_tree])
        # idle_type_ids = set(six.moves.filter(lambda it: it is not None, idle_type_ids))
        # Выключаем фильтр фактов простоев по доступным видам простоев
        # (пусть все видят все простои)
        # if idle_type_ids:
        # reason_q_arg = (Q(supervisor_reason_id__in=idle_type_ids)
        #               | Q(driver_reason_id__in=idle_type_ids)
        #               | Q(automatic_reason_id__in=idle_type_ids))
        # null_reason_q_arg = (Q(supervisor_reason_id__isnull=True)
        #                    & Q(driver_reason_id__isnull=True)
        #                    & Q(automatic_reason_id__isnull=True))
        # idle_qs = idle_qs.filter(reason_q_arg | null_reason_q_arg)

        return idle_qs

    def get_data(self, request):
        return self.j(self.get_qs_data(request, report=True)[0])

    def filter(self, request):
        queryset, total = super(IdleReportView, self).filter(request)
        # Так как таблица позиций пустая, то выключим
        # self.get_positions(queryset)
        return queryset, total

    def get_positions(self, queryset):
        if getattr(self, "_positions", None) is None:
            self._positions = defaultdict(list)
            idle_positions = IdlePositions.objects \
                .select_related("geometry") \
                .filter(idle__in=queryset) \
                .only("geometry__name", "idle_id", "geometry_id")
            for ip in idle_positions:
                geometry_name = ip.geometry.name if ip.geometry_id else ""
                if geometry_name not in self._positions[ip.idle_id]:
                    self._positions[ip.idle_id].append(geometry_name)
            if CALC_IDLE_GEOMETRIES:  # если в сеттингсах не отключен расчёт
                not_positioned = set(
                    i.id for i in queryset).difference(self._positions)
                if not_positioned:
                    idles = [i for i in queryset if i.id in not_positioned]
                    created = []
                    enterprises_ids = {i.vehicle.enterprise_id for i in idles}
                    geometries = defaultdict(list)
                    for g in Geometry.objects.filter(enterprise_id__in=enterprises_ids):
                        try:
                            polygon = shapely_geometry.Polygon(
                                shapely_geometry.LinearRing([
                                    Point(lon, lat).xy
                                    for lon, lat in g.wkt.exterior.coords
                                ]))
                            geometries[g.enterprise_id].append(
                                {"p": polygon, "id": g.id, "name": g.name}
                            )
                        except:
                            pass
                    for idle in idles:
                        idle.positioned = False
                        point = shapely_geometry.Point(
                            *Point(idle.lon, idle.lat).xy)
                        for item in geometries[idle.vehicle.enterprise_id]:
                            if item["p"].contains(point):
                                created.append(IdlePositions(
                                    idle=idle, geometry_id=item["id"]))
                                self._positions[idle.id].append(item["name"])
                                idle.positioned = True
                    IdlePositions.objects.bulk_create(created)
                    created = []
                    for idle in idles:
                        if not idle.positioned:
                            created.append(IdlePositions(
                                idle=idle, geometry=None))
                            self._positions[idle.id].append("")
                    IdlePositions.objects.bulk_create(created)
        return self._positions

    def calculate_geometries(self, raw):
        if getattr(self, "_positions", None) is None:
            queryset = Idle.objects.filter(id=raw["id"])
            self.get_positions(queryset)
        geometries = self._positions.get(raw["id"], [])
        return ", ".join(geometries)

    def get_qs_data(self, request, report=False):
        if not report:
            return [], 0
        return super(IdleReportView, self).get_qs_data(request, report)

    def get_context_data(self, **kwargs):
        context = super(IdleReportView, self).get_context_data(**kwargs)

        # у какого типа простоя какие виды техники
        idle_types_codes = defaultdict(list)
        idle_veh_kinds_qs = IdleVehicleKind.objects.filter(
            EnterprisesControl.Q("idle_type__enterprise",
                                 self.filters_data.vehicle__enterprise,
                                 request=self.request) |
            Q(idle_type__enterprise_id__isnull=True)
        )
        for ivk in idle_veh_kinds_qs:
            idle_types_codes[ivk.idle_type_id].append(ivk.vehicle_kind_code)

        # кактегории простоев
        category_qs = OrganizationCategory.objects.filter(
            EnterprisesControl.Q("enterprise",
                                 self.filters_data.vehicle__enterprise,
                                 request=self.request)
        )
        category_list = [cat.__json__() for cat in category_qs]
        category_list.insert(0, {"id": None, "name": _("Без категории")})
        category_list.insert(0, {"id": -1, "name": _("Все")})

        # типы простоев по видам техники и предприятиям
        ent_id = self.filters_data.vehicle__enterprise
        all_idle_types = []
        idle_types = defaultdict(lambda: defaultdict(list))
        idle_type_qs = IdleType.objects.filter(
            Q(enterprise_id__in=self.logic_user.enterprises_ids) |
            Q(enterprise_id__isnull=True)
        ).select_related("organization_category").prefetch_related("vehicle_type", "vehicle_model")
        category_qs = OrganizationCategory.objects.all()
        if ent_id:
            category_qs = category_qs.filter(enterprise_id=ent_id)
        all_idle_categories = list(category_qs.values("id", "name"))
        for it in idle_type_qs:
            if ent_id == 0 or not it.enterprise_id or (it.enterprise_id == ent_id):
                all_idle_types.append(dict(
                    id=it.id,
                    name=it.name,
                    category=it.organization_category_id,
                    category__name=it.organization_category.name if it.organization_category else _(
                        "Без категории"),
                ))
            for veh_kind_code in idle_types_codes[it.id]:
                idle_types[veh_kind_code][it.enterprise_id if it.enterprise_id else 0].append(dict(
                    id=it.id,
                    name=it.name,
                    category=it.organization_category_id,
                    category__name=it.organization_category.name if it.organization_category else _(
                        "Без категории"),
                    type_list=[t.id for t in it.vehicle_type.all()],
                    model_list=[m.id for m in it.vehicle_model.all()],
                ))

        # продолжительности нормативных простоев
        idle_type_norms = {}
        n_qs = IdleTypeNorming.objects.all()
        if ent_id:
            n_qs = n_qs.filter(Q(idle_type__enterprise__isnull=True) | Q(
                idle_type__enterprise_id=ent_id))
        for idle_norm in n_qs:
            params = json.loads(idle_norm.params) if idle_norm.params else {}
            key = "%s:%s:%s" % (idle_norm.idle_type_id, params.get("model", "") or "",
                                params.get("unload_type", "") or "",)
            idle_type_norms[key] = idle_norm.duration

        # мобильные объекты по видам техники
        vehicles = sorted([dict(
            id=v.id,
            name=v.name,
            enterprise_id=v.enterprise_id,
            vehicle_kind=v.vehicle_kind_codes,
            vehicle_model=v.model_id,
            vehicle_type=v.vehicle_type_id,
        ) for v in six.itervalues(self.vehicles_dict)], key=lambda obj: obj.get('name'))

        # ЗАПЧАСТИ для простоев
        vehicleparts_qs = VehiclePart.objects.filter(
            EnterprisesControl.Q("enterprise",
                                 self.filters_data.vehicle__enterprise,
                                 request=self.request)
        )
        vehicleparts_list = defaultdict(lambda: defaultdict(list))

        vp_types_codes = defaultdict(list)
        vp_veh_kinds_qs = VehicleKindPart.objects.filter(
            EnterprisesControl.Q("vehicle_part__enterprise",
                                 self.filters_data.vehicle__enterprise,
                                 request=self.request) |
            Q(vehicle_part__enterprise_id__isnull=True)
        )
        for vpvk in vp_veh_kinds_qs:
            vp_types_codes[vpvk.vehicle_part_id].append(vpvk.vehicle_kind_code)

        for vp in vehicleparts_qs:
            for veh_kind_code in vp_types_codes[vp.id]:
                vehicleparts_list[veh_kind_code][vp.enterprise_id if vp.enterprise_id else 0].append(dict(
                    id=vp.id,
                    name=vp.name
                ))
        for unique_vk in set(vpvk.vehicle_kind_code for vpvk in vp_veh_kinds_qs):
            vehicleparts_list[unique_vk][0].append(
                {"id": None, "name": _("-")})

        idle_logic = IdleTypeTreeLogic(self.filters_data.vehicle__enterprise)
        idle_tree = idle_logic.get_tree(user=self.request.user)
        # idle_tree = IdleTypeTreeLogic.get_leaves(idle_tree)

        # КОНЕЦ ЗАПЧАСТЕЙ для простоев
        allowed_enterprises = User.get_enterprises_ids(self.logic_user)
        ents = get_core_enterprise_settings_by_ids(allowed_enterprises)
        idle_merging_intervals = {
            ent.id: ent.idle_merging_interval for ent in ents}
        context.update(dict(
            vehicles=json.dumps(vehicles),
            idle_type_norms=json.dumps(idle_type_norms),
            idle_types=json.dumps(idle_types),
            all_idle_types=json.dumps(all_idle_types),
            all_idle_categories=json.dumps(all_idle_categories),
            category_list=json.dumps(category_list),
            vehicleparts_list=json.dumps(vehicleparts_list),
            show_vehicle_parts=json.dumps(SHOW_VEHICLE_PARTS),
            idle_merging_intervals=json.dumps(idle_merging_intervals),
            allow_auto_idle_delete=json.dumps(ALLOW_AUTO_IDLE_DELETE),
            idle_tree=json.dumps(idle_tree),
            has_classification=idle_logic.has_classification(),
        ))

        context["edit_idles_only_with_inoperable_asd"] = \
            self.edit_idles_only_with_inoperable_asd

        return context


class IdleReportAutoupdateView(IdleReportView):
    name = "core_idle_report_autoupdate_view"

    controls = {
        "vehicle__enterprise": EnterprisesControlRB,
        "vehicle": VehiclesControlRB,
        "shift_date": DateControl,
        "shift": WorkRegimeDetailControlRB,
        "time": IdleDurationControl,
    }

    def get_current_shift(self, enterprise_id):
        enterprise = get_core_enterprise_settings(enterprise_id)
        shift_date, current_shift = get_current_regime_detail(
            enterprise.accounting_shift)
        return current_shift

    def get_filters_data(self, request):
        enterprise_id = getattr(settings, 'DEFAULT_ENTERPRISE', None)
        all_vehicles = Vehicle.active.filter(
            enterprise_id=enterprise_id).values_list('id', flat=True)

        shift = self.get_current_shift(enterprise_id)

        request.GET = request.GET.copy()
        request.GET[EnterprisesControlRB.__name__] = '{}'.format(enterprise_id)
        request.GET[VehiclesControlRB.__name__] = '{}'.format(
            list(all_vehicles))
        request.GET[DateControl.__name__] = '"{}"'.format(
            shift.shift_date.strftime('%Y-%m-%d %H:%M'))
        request.GET[WorkRegimeDetailControlRB.__name__] = '{}'.format(
            shift.workregime_detail.id)
        request.GET[IdleDurationControl.__name__] = '{}'.format(0)

        short_link_full = urlencode({
            k.__name__: request.GET[k.__name__] for k in [EnterprisesControlRB, DateControl, VehiclesControlRB, WorkRegimeDetailControlRB, IdleDurationControl]
        }).replace("+", "%20")
        _hash = hashlib.md5()
        _hash.update(short_link_full)
        _hash = _hash.hexdigest()
        short_link_short = self.request.GET.get('short_link', _hash)
        try:
            short_link, _ = ShortLinks.objects.get_or_create(
                short=short_link_short)
            short_link.full = force_text(short_link_full)
            short_link.save()
        except Exception as e:
            pass

        self.request.GET = request.GET.copy()
        self.request.GET['short_link'] = short_link_short

        return super(IdleReportAutoupdateView, self).get_filters_data(request)

    def get_context_data(self, **kwargs):
        context = super(IdleReportAutoupdateView,
                        self).get_context_data(**kwargs)
        context['short_link'] = self.request.GET.get('short_link')
        context['force_reload'] = True

        enterprise_id = getattr(settings, 'DEFAULT_ENTERPRISE', None)
        enterprise = get_core_enterprise_settings(enterprise_id)
        now = timezone_now().astimezone(pytz.timezone(enterprise.time_zone))
        current_regime = enterprise.accounting_shift
        shift_date, current_shift = get_current_regime_detail(
            current_regime, now)
        _, shift_end = current_shift.borders
        context['seconds_till_next_shift'] = int(
            (shift_end-now).total_seconds())+10

        return context


class IdleReportViewModified(IdleReportView, QueryReport):
    name = "core_idle_report_view_modified"
    menu_name = __('Форма простоев core modified')
    verbose_name = __('Форма простоев core modified')
    description = __('Форма для работы с простоями modified')
    template_name = "idle_report_modified.jinja"
    usersort = True
    xl_template = ('reports/templates/core_idle_report_view_modified.xlsx')

    controls = {
        "vehicle__enterprise": EnterprisesControlRB,
        "vehicle": VehiclesControlRB,
        "shift_date": DateTimeRangeUberControlRB,
        "time": IdleDurationControl,
    }

    datafields = [
        # hidden
        {'name': 'id', 'type': 'int', "report": False, 'text': 'ID'},
        {'name': 'idle_type', 'type': 'string', 'report': False},
        {'name': 'closed', 'type': 'bool', "report": False},
        {"name": "reason", "type": "int", "report": False},
        {'name': 'automatic_reason_name', 'type': 'text', "report": False},
        {'name': 'driver_reason_name', 'type': 'text', "report": False},
        {'name': 'duration', 'type': 'text', "report": False},
        {'name': 'manual_created', 'type': 'int', "report": False},
        # visible
        {"name": "vehicle_name", "type": "string",
            "text": __("Мобильный объект")},
        {"name": "time_start", "type": "date", "text": __("Начало простоя"),
         "calculate_method": "get_time_start"},
        {"name": "time_stop", "type": "date", "text": __("Конец простоя"),
         "calculate_method": "get_time_stop"},
        {"name": "reason_name", "type": "string", "text": __("Тип")},
        {"name": "category_name", "type": "string", "text": __("Категория"),
         "calculate_method": "get_category_name"},
        {"name": "reason_code", "type": "string", "calculate_method": "calculate_reason_code",
         "text": "Автор типа простоя"},
        {"name": "supervisor_comment", "type": "string",
            "text": __("Описание причины")},
        {"name": "c_duration", "type": "datetime", "text": __(
            "Продолжительность"), "calculate_method": "convert_time"},
        {'name': 'manual', 'text': __(
            "Ручной"), 'type': 'string', "calculate_method": "get_manual"},
        {'name': 'split_cell', 'text': __(
            'Разделить простой'), "calculate_method": None, "report": False},
        {'name': 'geometries', 'text': __('Геозоны'), "calculate_method": "calculate_geometries",
         "report": False},
        {"name": 'vehicle_part_name',
            "type": "string", "text": __("Запчасть")},
        {'name': 'map_cell', 'text': __(
            'Показать на карте'), "calculate_method": None, "report": False},
    ]

    @property
    def period_range(self):
        return DateTimeRangeUberControlRB(self.request).cleaned_data

    def get_time_start(self, row):
        return max(row['time_start'], self.filters_data.get("shift_date")[0])

    def get_time_stop(self, row):
        return min(row['time_stop'], self.filters_data.get("shift_date")[1])

    def get_category_name(self, row):
        if row['category']:
            return OrganizationCategory.objects.get(id=row['category']).name
        else:
            return None

    def convert_time(self, row):
        start = self.get_time_start(row)
        stop = self.get_time_stop(row)
        duration = (stop - start).total_seconds()
        return timedelta(seconds=duration)

    def get_queryset(self, request):
        if USE_TRUE_IDLES:
            q_arg = (
                Q(closed=True, time_start__lt=self.period_range[1], time_stop__gt=self.period_range[0]) |
                Q(closed=False, time_start__lt=self.period_range[1])
            )
        else:
            q_arg = (
                Q(time_start__gte=self.period_range[0], time_start__lt=self.period_range[1]) |
                Q(time_stop__gte=self.period_range[0], time_stop__lt=self.period_range[1]) |
                Q(time_start__lt=self.period_range[0],
                  time_stop__gt=self.period_range[1])
            )
            if self.show_endless:
                q_arg |= Q(closed=False)
        idle_qs = Idle.objects.filter(q_arg).select_related(
            'vehicle', 'automatic_reason', 'driver_reason', 'supervisor_reason', 'vehicle_part',
            'automatic_reason__organization_category', 'driver_reason__organization_category'
        ).order_by('-time_stop')
        return idle_qs

    def get_xl_params(self):
        params = super(IdleReportViewModified, self).get_xl_params()
        params['title'] = __('Форма простоев')
        params['enterprise'] = self.filters_control['vehicle__enterprise'].verbose()
        params['date'] = self.filters_control['shift_date'].verbose()
        params['duration'] = self.filters_control['time'].verbose()
        params['vehicle'] = self.filters_control['vehicle'].verbose()

        params['enterprise_text'] = __('Предприятие')
        params['duration_text'] = __('Минимальная продолжительность, мин')
        params['date_text'] = __('Временной интервал')
        params['vehicle_text'] = __('Мобильный объект')

        return params


class ShiftVehicleReadinessView(ReportingMixin, AngularRestView):
    name = "core_shift_vehicle_readiness"
    verbose_name = _("Техническая готовность техники")
    context_object_name = "readiness_list"
    template_name = "readiness.jinja"

    controls = {
        'enterprise': EnterprisesControl,
        'date': DateControl,
        'regime': WorkRegimeDetailControl,
        'kind': VehicleKindControl,
    }

    @cached_property
    def date(self):
        if 'date' in self.filters_data:
            return self.filters_data.date.date()

    @cached_property
    def regime(self):
        if 'regime' in self.filters_data:
            return WorkRegimeDetail.objects.get(pk=self.filters_data.regime)

    @property
    def idle_type_list(self):
        itl = defaultdict(dict)
        for t in ['inner', 'full']:
            for k in VEHICLE_KINDS.values():
                itl[t][k.model.__name__.lower()] = list(IdleType.list(
                    enterprise=self.filters_data.enterprise, kind=t, vehicle_kind=k.model).values(
                    'id', 'name', 'organization_category__name'))
        return itl

    def get_context_data(self, **kwargs):

        def setType(obj):
            obj['objectType'] = None
            for k in kinds:
                if obj[k]:
                    obj['objectType'] = k
                del obj[k]
            return obj

        context = super(ShiftVehicleReadinessView,
                        self).get_context_data(**kwargs)
        context['idle_type_list'] = json.dumps(self.idle_type_list)
        kinds = VEHICLE_KINDS.kinds
        qs = Vehicle.objects.all() if self.filters_data.enterprise else Vehicle.objects.none()
        vehicle_qs = self.filter_queryset(qs)
        context['vehicle_list'] = json.dumps(
            [setType(v) for v in vehicle_qs.values('id', 'name', 'division', *kinds)])
        context['inner_idle_list'] = self.s(VehicleShiftIdle.objects.filter(
            regime=self.regime, date=self.date, vehicle__in=vehicle_qs))
        context['full_idle_list'] = self.s(VehicleFullShiftIdle.objects.filter(
            regime=self.regime, date=self.date, vehicle__in=vehicle_qs))
        context['regime'] = self.regime
        context['date'] = self.date
        context['inner_idle_url'] = reverse(
            ShiftVehicleReadinessInnerIdleView.get_hidden_slug_name())
        context['full_idle_url'] = reverse(
            ShiftVehicleReadinessFullIdleView.get_hidden_slug_name())
        context['division_list'] = self.s(Division.objects.filter(
            enterprise=self.filters_data.enterprise))
        return context


class VehicleShiftIdleForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        enterprises_ids = kwargs.pop("enterprises_ids")
        super(VehicleShiftIdleForm, self).__init__(*args, **kwargs)
        self.fields['vehicle'].queryset = Vehicle.objects.filter(
            enterprise__id__in=enterprises_ids)

    def clean(self):
        cleaned_data = super(VehicleShiftIdleForm, self).clean()
        instance = Context(cleaned_data)
        filter_kw = dict(date=instance.date,
                         regime=instance.regime, vehicle=instance.vehicle)
        if VehicleFullShiftIdle.objects.filter(**filter_kw).exists():
            raise ValidationError(_("Задан целосменный простой."))
        if VehicleShiftIdle.objects.filter(start__lt=instance.stop, stop__gt=instance.start, **filter_kw)\
                                   .exclude(id=self.instance.id).exists():
            raise ValidationError(_("Перескающиеся простои."))
        return cleaned_data

    class Meta(object):
        fields = '__all__'
        model = VehicleShiftIdle


class ShiftVehicleReadinessInnerIdleView(AngularRestView):
    name = "core_shift_vehicle_readiness_inner_view"
    parent = ShiftVehicleReadinessView
    create_form_class = VehicleShiftIdleForm
    update_form_class = VehicleShiftIdleForm
    emulate_delete = False

    def get_form_kwargs(self):
        kwargs = super(ShiftVehicleReadinessInnerIdleView,
                       self).get_form_kwargs()
        kwargs['enterprises_ids'] = self.logic_user.enterprises_ids
        return kwargs

    def get_queryset(self):
        return VehicleShiftIdle.objects.filter(vehicle__enterprise__id__in=self.logic_user.enterprises_ids)


class VehicleFullShiftIdleForm(forms.ModelForm):

    def __init__(self, *args, **kwargs):
        enterprises_ids = kwargs.pop("enterprises_ids")
        super(VehicleFullShiftIdleForm, self).__init__(*args, **kwargs)
        self.fields['vehicle'].queryset = Vehicle.objects.filter(
            enterprise__id__in=enterprises_ids)

    def save(self, commit=True):
        idle = super(VehicleFullShiftIdleForm, self).save(commit)
        VehicleShiftIdle.objects.filter(
            date=idle.date, regime=idle.regime, vehicle=idle.vehicle).delete()
        return idle

    class Meta(object):
        fields = '__all__'
        model = VehicleFullShiftIdle


class ShiftVehicleReadinessFullIdleView(AngularRestView):
    name = "core_shift_vehicle_readiness_full_view"
    parent = ShiftVehicleReadinessView
    create_form_class = VehicleFullShiftIdleForm
    update_form_class = VehicleFullShiftIdleForm
    emulate_delete = False

    def get_form_kwargs(self):
        kwargs = super(ShiftVehicleReadinessFullIdleView,
                       self).get_form_kwargs()
        kwargs['enterprises_ids'] = self.logic_user.enterprises_ids
        return kwargs

    def get_queryset(self):
        return VehicleFullShiftIdle.objects.filter(vehicle__enterprise__id__in=self.logic_user.enterprises_ids)


class FMMappingForm(forms.ModelForm):
    class Meta(object):
        fields = '__all__'
        model = IdleTypeToFMCode

    def __init__(self, *args, **kwargs):
        self.enterprise_id = kwargs.pop('enterprise_id')
        super(FMMappingForm, self).__init__(*args, **kwargs)

    def clean_fm_code(self):
        # Коды, Занятые для ВГ и ВР
        fm_code = self.cleaned_data['fm_code']
        blocked_codes = LoadTypeToFMCode.objects.filter(enterprise_id=self.enterprise_id)\
            .values_list('code', flat=True)
        if fm_code in blocked_codes:
            raise ValidationError(
                __('Этот код контроллера уже зарегистрирован для вида груза и вида работ'))
        return fm_code


class IdleTypeToFMView(AngularDictionaryView):
    name = "core_idle_type_to_fm_mapping"
    verbose_name = _("Соответствие типов простоя кодам контроллера")
    description = _("Коды простоев контроллеров")
    template_name = "idle_type_to_fm_mapping.jinja"
    create_form_class = FMMappingForm
    update_form_class = FMMappingForm
    emulate_delete = False

    def get_queryset(self):
        return IdleTypeToFMCode.objects.select_related("idle_type").filter(idle_type__enterprise=self.enterprise)

    @cached_property
    def vehicle_kinds(self):
        qs = IdleVehicleKind.objects.filter(
            idle_type__enterprise_id=self.enterprise.id)
        res = defaultdict(list)
        for vk in qs:
            res[vk.idle_type_id].append(vk.vehicle_kind_code)
        return res

    @cached_property
    def vehicle_kinds_verbose(self):
        return dict(VEHICLE_KINDS.choices)

    def get_context_data(self, **kwargs):
        context = super(IdleTypeToFMView, self).get_context_data(**kwargs)
        idle_types = list(IdleType.objects.filter(
            enterprise_id=self.enterprise.id).values("id", "name"))
        for idle_type in idle_types:
            vehicle_kinds = self.vehicle_kinds[idle_type["id"]]
            vehicle_kinds_verbose = ", ".join([force_text(self.vehicle_kinds_verbose[code])
                                               for code in vehicle_kinds
                                               if code in self.vehicle_kinds_verbose])
            idle_type["name"] = "{} ({})".format(
                idle_type["name"], vehicle_kinds_verbose)

        context['idle_types'] = json.dumps(idle_types)
        return context

    def get_form_kwargs(self):
        data = super(IdleTypeToFMView, self).get_form_kwargs()
        data['enterprise_id'] = self.enterprise.id
        return data


class SecondaryOperationForm(forms.ModelForm):

    class Meta(object):
        model = SecondaryOperation
        fields = ["code", "name"]


class SecondaryOperationView(AngularDictionaryView):
    name = "core_secondary_operations"
    verbose_name = _("Справочник вспомогательных тех. операций")
    description = _("Справочник вспомогательных тех. операций")
    menu_name = _("Справочник вспомогательных тех. операций")
    template_name = "secondary_operations.jinja"
    update_form_class = SecondaryOperationForm
    create_form_class = SecondaryOperationForm
    emulate_delete = True

    def get_queryset(self):
        return SecondaryOperation.objects.filter(enterprise_id=self.enterprise.id)

    def create_object(self, create_form, commit=True):
        create_form.instance.enterprise_id = self.enterprise.id
        return super(SecondaryOperationView, self).create_object(create_form, commit)


