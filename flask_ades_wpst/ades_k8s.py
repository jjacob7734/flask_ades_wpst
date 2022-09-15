import os
import re
import json
import yaml
import base64
import string
import random
import requests
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from flask_ades_wpst.ades_abc import ADES_ABC


class ADES_K8s(ADES_ABC):
    def __init__(self, ades_id):
        print(f"in ADES_K8s.__init__()")
        self._ades_id = ades_id

        # detect if debugging K8s
        self.debug_k8s = os.environ.get("DEBUG_K8S", "false").lower() == "true"
        print(f"self.debug_k8s: {self.debug_k8s}")

        # detect namespace
        self.ns = os.environ.get("NAMESPACE", "ades").lower()
        print(f"self.ns: {self.ns}")

        # detect storage class
        self.sc_name = os.environ.get("STORAGE_CLASS", None)
        print(f"self.sc_name: {self.sc_name}")

        # detect using NFS for PersistentVolumes
        self.use_nfs = os.environ.get("USE_NFS", None)
        print(f"self.use_nfs: {self.use_nfs}")

        # detect using EFS for PersistentVolumes
        self.use_efs = os.environ.get("USE_EFS", None)
        print(f"self.use_efs: {self.use_efs}")

        # get k8s client
        config.load_kube_config()
        self.core_client = client.CoreV1Api()

        # create namespace
        try:
            body = client.V1Namespace(metadata=client.V1ObjectMeta(name=self.ns))
            api_response = self.core_client.create_namespace(body)
        except ApiException as e:
            if e.status not in (403, 409):
                raise

        # create namespaced role (pod-manager-role) and role binding
        # (pod-manager-default-binding) for calrissian pod management
        self.rbac_client = client.RbacAuthorizationV1Api()
        try:
            pod_manager_rules = [
                client.V1PolicyRule(
                    [""],
                    resources=["pods"],
                    verbs=["create", "patch", "delete", "list", "watch"],
                )
            ]
            pod_manager_role = client.V1Role(rules=pod_manager_rules)
            pod_manager_role.metadata = client.V1ObjectMeta(
                namespace=self.ns, name="pod-manager-role"
            )
            self.rbac_client.create_namespaced_role(self.ns, pod_manager_role)
        except ApiException as e:
            if e.status != 409:
                raise
        try:
            pod_manager_binding = client.V1RoleBinding(
                metadata=client.V1ObjectMeta(
                    namespace=self.ns, name="pod-manager-default-binding"
                ),
                subjects=[
                    client.V1Subject(
                        kind="ServiceAccount", namespace=self.ns, name="default"
                    )
                ],
                role_ref=client.V1RoleRef(
                    kind="Role",
                    api_group="rbac.authorization.k8s.io",
                    name="pod-manager-role",
                ),
            )
            self.rbac_client.create_namespaced_role_binding(
                namespace=self.ns, body=pod_manager_binding
            )
        except ApiException as e:
            if e.status != 409:
                raise

        # create namespaced role (pod-manager-role) and role binding
        # (pod-manager-default-binding) for calrissian log access
        try:
            log_reader_rules = [
                client.V1PolicyRule([""], resources=["pods/log"], verbs=["get", "list"])
            ]
            log_reader_role = client.V1Role(rules=log_reader_rules)
            log_reader_role.metadata = client.V1ObjectMeta(
                namespace=self.ns, name="log-reader-role"
            )
            self.rbac_client.create_namespaced_role(self.ns, log_reader_role)
        except ApiException as e:
            if e.status != 409:
                raise
        try:
            log_reader_binding = client.V1RoleBinding(
                metadata=client.V1ObjectMeta(
                    namespace=self.ns, name="log-reader-default-binding"
                ),
                subjects=[
                    client.V1Subject(
                        kind="ServiceAccount", namespace=self.ns, name="default"
                    )
                ],
                role_ref=client.V1RoleRef(
                    kind="Role",
                    api_group="rbac.authorization.k8s.io",
                    name="log-reader-role",
                ),
            )
            self.rbac_client.create_namespaced_role_binding(
                namespace=self.ns, body=log_reader_binding
            )
        except ApiException as e:
            if e.status != 409:
                raise

        # create k8s secret for cloud object store
        self.cloud_creds = dict()
        if "S3_AWS_ACCESS_KEY_ID" in os.environ:
            self.cloud_creds["aws"] = {
                "aws_access_key_id": base64.b64encode(
                    os.environ.get("S3_AWS_ACCESS_KEY_ID").encode()
                ).decode("utf-8"),
                "aws_secret_access_key": base64.b64encode(
                    os.environ.get("S3_AWS_SECRET_ACCESS_KEY").encode()
                ).decode("utf-8"),
            }
        for cloud in self.cloud_creds:
            try:
                sec = client.V1Secret()
                sec.metadata = client.V1ObjectMeta(name=f"{cloud}-creds")
                sec.type = "Opaque"
                sec.immutable = True
                sec.data = self.cloud_creds[cloud]
                self.core_client.create_namespaced_secret(namespace=self.ns, body=sec)
            except ApiException as e:
                if e.status != 409:
                    raise

    def id_generator(self, size=12, chars=string.ascii_lowercase + string.digits):
        """K8s-compatible ID generator."""
        return "".join(random.choice(chars) for _ in range(size))

    def deploy_proc(self, proc_spec):
        return proc_spec

    def undeploy_proc(self, proc_spec):
        return proc_spec

    def exec_job(self, job_spec):
        print(f"in ADES_K8s.exec_job(): job_spec={json.dumps(job_spec, indent=2)}")

        # default resource specs
        resource_req_defaults = {
            "coresMin": 1,
            "ramMin": 1024,
            "tmpdirMin": 1000,
            "outdirMin": 1000,
        }

        # get cwl
        r = requests.get(job_spec["process"]["owsContextURL"], verify=False)
        r.raise_for_status()
        cwl = yaml.safe_load(r.text)
        resource_req = {
            **resource_req_defaults,
            **(cwl.get("requirements", {}).get("ResourceRequirement", {})),
        }
        print(
            f"in ADES_K8s.exec_job(): ResourceRequirement={json.dumps(resource_req, indent=2)}"
        )

        # generate unique ID that conforms to K8s requirements (<63 alphanumeric chars, -, _)
        id = self.id_generator()

        # print("Listing pods with their IPs:")
        # ret = self.core_client.list_pod_for_all_namespaces(watch=False)
        # for i in ret.items:
        #    print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
        # for pod in self.core_client.list_namespaced_pod(self.ns).items:
        #    print("%s\t%s\t%s" % (pod.status.pod_ip, pod.metadata.namespace, pod.metadata.name))

        # create PVC for tmpout
        tmpout_pvc_name = f"tmpout-{id}"
        if self.use_nfs:
            tmpout_pv_name = f"{tmpout_pvc_name}-pv"
            if self.use_efs:
                body = client.V1PersistentVolume(
                    metadata=client.V1ObjectMeta(name=tmpout_pv_name),
                    spec={
                        "accessModes": ["ReadWriteMany"],
                        "capacity": {"storage": f"{resource_req['tmpdirMin']}Mi"},
                        "volumeMode": "Filesystem",
                        "persistentVolumeReclaimPolicy": "Retain",
                        "storageClassName": self.sc_name,
                        "csi": {
                            "driver": "efs.csi.aws.com",
                            "volumeHandle": "fs-0dee47898008e38a9",
                        },
                    },
                )
                self.core_client.create_persistent_volume(body=body)
                body = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=tmpout_pvc_name),
                    spec={
                        "accessModes": ["ReadWriteMany"],
                        "resources": {
                            "requests": {"storage": f"{resource_req['tmpdirMin']}Mi"}
                        },
                        "volumeName": tmpout_pv_name,
                        "storageClassName": self.sc_name,
                    },
                )
            else:
                body = client.V1PersistentVolume(
                    metadata=client.V1ObjectMeta(name=tmpout_pv_name),
                    spec={
                        "accessModes": ["ReadOnlyMany"],
                        "capacity": {"storage": f"{resource_req['tmpdirMin']}Mi"},
                        "nfs": {"server": self.use_nfs, "path": "/"},
                    },
                )
                self.core_client.create_persistent_volume(body=body)
                body = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=tmpout_pvc_name),
                    spec={
                        "accessModes": ["ReadOnlyMany"],
                        "resources": {
                            "requests": {"storage": f"{resource_req['tmpdirMin']}Mi"}
                        },
                        "volumeName": tmpout_pv_name,
                        "storageClassName": self.sc_name,
                    },
                )
        else:
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=tmpout_pvc_name),
                spec={
                    "accessModes": ["ReadWriteMany"],
                    "resources": {
                        "requests": {"storage": f"{resource_req['tmpdirMin']}Mi"}
                    },
                    "storageClassName": self.sc_name,
                },
            )
        self.core_client.create_namespaced_persistent_volume_claim(
            namespace=self.ns, body=body
        )

        # create PVC for output data
        output_pvc_name = f"output-data-{id}"
        if self.use_nfs:
            output_pv_name = f"{output_pvc_name}-pv"
            if self.use_efs:
                body = client.V1PersistentVolume(
                    metadata=client.V1ObjectMeta(name=output_pv_name),
                    spec={
                        "accessModes": ["ReadWriteMany"],
                        "capacity": {"storage": f"{resource_req['outdirMin']}Mi"},
                        "volumeMode": "Filesystem",
                        "persistentVolumeReclaimPolicy": "Retain",
                        "storageClassName": self.sc_name,
                        "csi": {
                            "driver": "efs.csi.aws.com",
                            "volumeHandle": "fs-020c1f2cc4d705a8e",
                        },
                    },
                )
                self.core_client.create_persistent_volume(body=body)
                body = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=output_pvc_name),
                    spec={
                        "accessModes": ["ReadWriteMany"],
                        "resources": {
                            "requests": {"storage": f"{resource_req['outdirMin']}Mi"}
                        },
                        "volumeName": output_pv_name,
                        "storageClassName": self.sc_name,
                    },
                )
            else:
                body = client.V1PersistentVolume(
                    metadata=client.V1ObjectMeta(name=output_pv_name),
                    spec={
                        "accessModes": ["ReadOnlyMany"],
                        "capacity": {"storage": f"{resource_req['outdirMin']}Mi"},
                        "nfs": {"server": self.use_nfs, "path": "/"},
                    },
                )
                self.core_client.create_persistent_volume(body=body)
                body = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=output_pvc_name),
                    spec={
                        "accessModes": ["ReadOnlyMany"],
                        "resources": {
                            "requests": {"storage": f"{resource_req['outdirMin']}Mi"}
                        },
                        "volumeName": output_pv_name,
                        "storageClassName": self.sc_name,
                    },
                )
        else:
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=output_pvc_name),
                spec={
                    "accessModes": ["ReadWriteMany"],
                    "resources": {
                        "requests": {"storage": f"{resource_req['outdirMin']}Mi"}
                    },
                    "storageClassName": self.sc_name,
                },
            )
        self.core_client.create_namespaced_persistent_volume_claim(
            namespace=self.ns, body=body
        )

        # create job
        k8s_job_spec = {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "calrissian-job",
                            "image": "pymonger/calrissian:latest",
                            "imagePullPolicy": "IfNotPresent",
                            "envFrom": [{"secretRef": {"name": "aws-creds"}}],
                            "command": ["/app/run_and_dump_usage.sh"],
                            "args": [
                                "--stdout",
                                f"/calrissian/output-data/{output_pvc_name}/docker-output.json",
                                "--stderr",
                                f"/calrissian/output-data/{output_pvc_name}/docker-stderr.log",
                                "--max-ram",
                                f"{resource_req['ramMin']}Mi",
                                "--max-cores",
                                f"{resource_req['coresMin']}",
                                "--tmp-outdir-prefix",
                                f"/calrissian/tmpout/{tmpout_pvc_name}/",
                                "--outdir",
                                f"/calrissian/output-data/{output_pvc_name}/",
                                "--usage-report",
                                f"/calrissian/output-data/{output_pvc_name}/docker-usage.json",
                                job_spec["process"]["owsContextURL"],
                                f"/calrissian/tmpout/{tmpout_pvc_name}/inputs.json"
                            ],
                            "volumeMounts": [
                                {
                                    "mountPath": "/calrissian/tmpout",
                                    "name": tmpout_pvc_name,
                                },
                                {
                                    "mountPath": "/calrissian/output-data",
                                    "name": output_pvc_name,
                                },
                            ],
                            "env": [
                                {
                                    "name": "CALRISSIAN_POD_NAME",
                                    "valueFrom": {
                                        "fieldRef": {"fieldPath": "metadata.name"}
                                    },
                                }
                            ],
                        }
                    ],
                    "restartPolicy": "Never",
                    "volumes": [
                        {
                            "name": tmpout_pvc_name,
                            "persistentVolumeClaim": {"claimName": tmpout_pvc_name},
                        },
                        {
                            "name": output_pvc_name,
                            "persistentVolumeClaim": {"claimName": output_pvc_name},
                        },
                    ],
                }
            }
        }

        # if debugging
        if self.debug_k8s:
            # don't retry k8s jobs
            k8s_job_spec["backoffLimit"] = 0

            # disable deletion of pods
            k8s_job_spec["template"]["spec"]["containers"][0]["env"].append(
                {"name": "CALRISSIAN_DELETE_PODS", "value": "false"}
            )

            # prepend calrissian options to help debugging
            for debug_opt in ["--leave-tmpdir", "--leave-outputs", "--debug"]:
                k8s_job_spec["template"]["spec"]["containers"][0]["args"].insert(
                    0, debug_opt
                )

        # add initContainer to prep up tmpout and output-data directories;
        # dump the input params into a JSON file to support complex input params 
        k8s_job_spec["template"]["spec"]["initContainers"] = [
            {
                "name": "init-volumes",
                "image": "busybox",
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh"],
                "args": [
                    "-c",
                    "chmod 777 /calrissian || true && "
                    + "chmod +t /calrissian || true && "
                    + f"mkdir -p /calrissian/tmpout/{tmpout_pvc_name} && "
                    + f"chmod 777 /calrissian/tmpout/{tmpout_pvc_name} && "
                    + f"chmod +t /calrissian/tmpout/{tmpout_pvc_name} && "
                    + f"mkdir -p /calrissian/output-data/{output_pvc_name} && "
                    + f"chmod 777 /calrissian/output-data/{output_pvc_name} && "
                    + f"chmod +t /calrissian/output-data/{output_pvc_name} && "
                    + f"cat << EOF > /calrissian/tmpout/{tmpout_pvc_name}/inputs.json\n"
                    + f"{json.dumps(job_spec['inputs'], indent=2)}\nEOF"
                ],
                "volumeMounts": [
                    {"mountPath": "/calrissian/tmpout", "name": tmpout_pvc_name,},
                    {"mountPath": "/calrissian/output-data", "name": output_pvc_name,},
                ],
            }
        ]

        # create job_id and prepend to script inputs
        job_id = f"calrissian-job-{id}"
        k8s_job_spec["template"]["spec"]["containers"][0]["args"].insert(0, job_id)

        # submit job
        body = client.V1Job()
        body.metadata = client.V1ObjectMeta(namespace=self.ns, name=job_id)
        body.status = client.V1JobStatus()
        body.spec = k8s_job_spec
        batch_api = client.BatchV1Api()
        api_response = batch_api.create_namespaced_job(
            namespace=self.ns, body=body, pretty=True
        )
        api_response = client.ApiClient().sanitize_for_serialization(api_response)
        print(f"api_response: {api_response}")

        return {
            "status": "accepted",
            "k8s_tmpout_pvc_name": tmpout_pvc_name,
            "k8s_output_pvc_name": output_pvc_name,
            "k8s_job_id": job_id,
            "api_response": api_response,
        }

    def dismiss_job(self, job_spec):
        # get job id
        k8s_job_id = job_spec["backend_info"]["k8s_job_id"]

        # get job status
        status = self.get_job(job_spec)["status"]
        if status not in ("running", "accepted"):
            raise RuntimeError(f"Cannot dismiss job {k8s_job_id} with status {status}.")

        # delete the job
        batch_api = client.BatchV1Api()
        api_response = batch_api.delete_namespaced_job(
            name=k8s_job_id, namespace=self.ns, pretty=True
        )
        print(api_response.status)
        return job_spec

    def get_job(self, job_spec):
        # print(f"backend_info: {job_spec['backend_info']}")
        # print(f"type(backend_info): {type(job_spec['backend_info'])}")
        # print(f"job_spec: {json.dumps(job_spec, indent=2)}")
        k8s_job_id = job_spec["backend_info"]["k8s_job_id"]
        batch_api = client.BatchV1Api()
        job_info = batch_api.read_namespaced_job(
            name=k8s_job_id, namespace=self.ns, pretty=True
        )
        # print(job_info.status)
        job_info_sanitized = client.ApiClient().sanitize_for_serialization(job_info)
        # print(f"job_info_sanitized: {json.dumps(job_info_sanitized, indent=2)}")

        # determine K8s job state:
        # https://v1-19.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#job-v1-batch
        # and map to ADES job state:
        # https://raw.githubusercontent.com/opengeospatial/wps-rest-binding/master/core/openapi/schemas/statusCode.yaml
        conditions = job_info.status.conditions
        # print(f"condtions: {conditions}")
        if isinstance(conditions, list):
            for condition in conditions:
                if condition.type == "Complete" and condition.status:
                    job_spec["status"] = "successful"
                    break
                elif condition.type == "Failed" and condition.status:
                    job_spec["status"] = "failed"
                    break
                else:
                    raise NotImplemented(f"Unhandled condition: {condition}")
        elif conditions is None:
            if job_info.status.active > 0:
                job_spec["status"] = "running"
        else:
            raise NotImplemented(f"Unhandled condition type: {type(conditions)}")

        # set default job log
        job_log = ""

        # set empty usage stats
        usage_stats = dict()

        # get info on all pods related to this job
        pod_info_dict = {}
        pod_label_selector = "job-name=" + k8s_job_id
        pods_list = self.core_client.list_namespaced_pod(
            namespace=self.ns, label_selector=pod_label_selector
        )
        for pod_info in pods_list.items:
            pod_info_sanitized = client.ApiClient().sanitize_for_serialization(pod_info)
            # print(f"pod_info_sanitized: {json.dumps(pod_info_sanitized, indent=2)}")

            # identify job pod and process differently
            if "controller-uid" in pod_info_sanitized["metadata"]["labels"]:
                # get job log
                try:
                    job_log = self.core_client.read_namespaced_pod_log(
                        name=pod_info_sanitized["metadata"]["name"],
                        namespace=self.ns,
                        pretty=True,
                    )
                    # print(job_log)
                except ApiException:
                    pass

                # extract docker usage stats from job log
                usage_re = re.compile(
                    r"# BEGIN docker-usage.json\n(.*)\n# END docker-usage.json",
                    re.DOTALL,
                )
                match = usage_re.search(job_log)
                usage_stats = json.loads(match.group(1)) if match else dict()
                # print(json.dumps(usage_stats, indent=2, sort_keys=True))
            else:
                pod_info_dict[
                    pod_info_sanitized["spec"]["containers"][0]["name"]
                ] = pod_info_sanitized

        # collect metrics from usage stats
        if len(usage_stats) > 0:
            metrics = {
                "username": job_spec["jobOwner"],
                "job_id": job_spec["jobID"],
                "job_type": job_spec["procID"],
                "job_status": job_spec["status"],
                "workflow": {
                    "time_queued": job_info_sanitized["status"]["startTime"],
                    "time_start": usage_stats["start_time"],
                    "time_end": usage_stats["finish_time"],
                    "time_duration_seconds": usage_stats["elapsed_seconds"],
                    "work_dir_size_gb": usage_stats["total_disk_megabytes"]/1024.,
                    "memory_max_gb": usage_stats["max_parallel_ram_megabytes"]/1024.,
                    "exit_code": 0 if job_spec["status"] == "successful" else 1,
                    "node": {
                        "node_type": "unknown",
                        "cores": usage_stats["cores_allowed"],
                        "memory_gb": usage_stats["ram_mb_allowed"]/1024.,
                        "hostname": "unknown",
                        "ip_address": "unknown",
                        "disk_space_free_gb": "unknown",
                    }
                },
                # TODO: delete this original implementation that handled processes generically
                # "processes": [
                #     {
                #         "name": child["name"],
                #         "time_started": child["start_time"],
                #         "time_end": child["finish_time"],
                #         "work_dir_size_gb": child["disk_megabytes"] / 1024.0,
                #         "memory_max_gb": child["ram_megabytes"] / 1024.0,
                #         "node": {
                #             "cores": child["cpus"],
                #             "memory_gb": "unknown",
                #             "hostname": pod_info_dict[
                #                 f"{child['name'].replace('_', '-')}-container"
                #             ]["status"]["podIP"],
                #             "ip_address": pod_info_dict[
                #                 f"{child['name'].replace('_', '-')}-container"
                #             ]["status"]["podIP"],
                #             "disk_space_free_gb": "unknown",
                #         },
                #     }
                #     for child in usage_stats["children"]
                # ],
                "stage-in": {
                    "time_start": usage_stats["children"][0]["start_time"],
                    "time_end": usage_stats["children"][0]["finish_time"],
                    "time_duration_seconds": usage_stats["children"][0]["elapsed_seconds"],
                    "work_dir_size_gb": usage_stats["children"][0]["disk_megabytes"]/1024.,
                    "memory_max_gb": usage_stats["children"][0]["ram_megabytes"]/1024.,
                    "exit_code": 0 if job_spec["status"] == "successful" else 1,
                    "node": {
                        "node_type": "unknown",
                        "cores": usage_stats["children"][0]["cpus"],
                        "memory_gb": "unknown",
                        "hostname": "unknown",
                        "ip_address": "unknown",
                        "disk_space_free_gb": "unknown",
                    }
                },
                "process": {
                    "time_start": usage_stats["children"][1]["start_time"],
                    "time_end": usage_stats["children"][1]["finish_time"],
                    "time_duration_seconds": usage_stats["children"][1]["elapsed_seconds"],
                    "work_dir_size_gb": usage_stats["children"][1]["disk_megabytes"]/1024.,
                    "memory_max_gb": usage_stats["children"][1]["ram_megabytes"]/1024.,
                    "exit_code": 0 if job_spec["status"] == "successful" else 1,
                    "node": {
                        "node_type": "unknown",
                        "cores": usage_stats["children"][1]["cpus"],
                        "memory_gb": "unknown",
                        "hostname": "unknown",
                        "ip_address": "unknown",
                        "disk_space_free_gb": "unknown",
                    }
                },
                "stage-out": {
                    "time_start": usage_stats["children"][2]["start_time"],
                    "time_end": usage_stats["children"][2]["finish_time"],
                    "time_duration_seconds": usage_stats["children"][2]["elapsed_seconds"],
                    "work_dir_size_gb": usage_stats["children"][2]["disk_megabytes"]/1024.,
                    "memory_max_gb": usage_stats["children"][2]["ram_megabytes"]/1024.,
                    "exit_code": 0 if job_spec["status"] == "successful" else 1,
                    "node": {
                        "node_type": "unknown",
                        "cores": usage_stats["children"][2]["cpus"],
                        "memory_gb": "unknown",
                        "hostname": "unknown",
                        "ip_address": "unknown",
                        "disk_space_free_gb": "unknown",
                    }
                },
                "blob": usage_stats,
            }
            job_spec["metrics"] = metrics
            # print(json.dumps(job_spec["metrics"], indent=2, sort_keys=True))

        return job_spec

    def get_job_results(self, job_spec):
        return {}
