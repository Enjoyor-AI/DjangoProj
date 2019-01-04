from django.conf.urls import *
from django.contrib import admin
from . controller import search, Interface
from .controller import  schedular_task

# 注册路由
urlpatterns = [
    # url(r'^getJson$', search.getJson),
    url(r'^getJson1$', search.getJson1),
    url(r'^startInterface$', Interface.getJson2),
    url(r'^getOperate$', Interface.getOperate),
    url(r'^admin/', admin.site.urls),
    url(r'^getOperateRatio/', Interface.getOperateRatio),
    url(r'^getAlarmAuto$', Interface.getAlarmAuto),

    url(r'^getAvailability', Interface.getAvailability),

    url(r'^getDetectorList', Interface.getDetectorList),
    url(r'^getDetectorDetail', Interface.getDetectorDetail),
    url(r'^getDetectorPlot', Interface.getDetectorPlot),
    url(r'^addMaintainList', Interface.addMaintainList),
    url(r'^getMaintainList', Interface.getMaintainList),
    url(r'^deleteMaintainList', Interface.deleteMaintainList),
    url(r'^exportOriginalList', Interface.exportOriginalList),
    url(r'^exportCurrentListState', Interface.exportCurrentListState),
    url(r'^getAbnormalIntersect', Interface.getAbnormalIntersect),

    url(r'^getDetectorMerge$', Interface.getDetectorMerge),
    url(r'^testDetectorList$', Interface.testDetectorList),

    # url(r'^exportDetectorReport$', Interface.exportDetectorReport),
    # url(r'^demo/guest/getState$', Interface.demo),
    # url(r'^taskOperate$', schedular_task.getScheduler),
    url(r'^demo/scats/getInterface$', Interface.getInterfaceStatus),
    url(r'^demo/scats/getParseFailed$', Interface.getParseFailed),
    url(r'^demo/scats/getScheTask$', Interface.getScheTask),
    url(r'^demo/scats/getLaneStatus$', Interface.getLaneStatus),
    url(r'^demo/scats/getRequestManage$', Interface.getRequestManage),
    url(r'^pagedisplay$', Interface.pagedisplay),
    # url(r'^runOperate$', Interface.runOperate)
]