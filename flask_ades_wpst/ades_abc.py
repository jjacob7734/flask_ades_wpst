from abc import ABCMeta, abstractmethod


class ADES_ABC(metaclass=ABCMeta):

    @abstractmethod
    def deploy_proc(self, proc_spec):
        raise NotImplementedError()

    @abstractmethod
    def undeploy_proc(self, proc_spec):
        raise NotImplementedError()

    @abstractmethod
    def exec_job(self, job_spec):
        raise NotImplementedError()

    @abstractmethod
    def dismiss_job(self, job_spec):
        raise NotImplementedError()

    @abstractmethod
    def get_job(self, job_spec):
        raise NotImplementedError()

    @abstractmethod
    def get_job_results(self, job_spec):
        raise NotImplementedError()
