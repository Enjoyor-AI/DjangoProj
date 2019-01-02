from proj.python_project.scats_interface.data_request_check import InterfaceCheck
import sys
sys.path.append("H:\GITHUB\DjangoProj\proj\proj\python_project")
from proj.python_project.scats_interface.condition_monitor import InterfaceStatus


if __name__ == "__main__":
    # I = InterfaceStatus()
    # I.salk_list_request('today')
    # I.operate_request('today')
    # I.salk_send(15)
    I2 = InterfaceStatus()
    I2.parse_failed_detector_send(15)
    # I2.salk_send(15)
    # I2.operate_send(15)
