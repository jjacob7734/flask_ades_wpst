import os
import json
import base64
import string
import random
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from flask_ades_wpst.ades_abc import ADES_ABC


class ADES_K8s(ADES_ABC):
    def __init__(self):
        print(f"in ADES_K8s.__init__()")

        # detect using NFS for PersistentVolumes
        self.use_nfs = os.environ.get("USE_NFS", None)
        print(f"self.use_nfs: {self.use_nfs}")

        # get k8s client
        config.load_kube_config()
        self.core_client = client.CoreV1Api()

        # create namespace
        self.ns = "soamc"
        try:
            body = client.V1Namespace(metadata=client.V1ObjectMeta(name=self.ns))
            api_response = self.core_client.create_namespace(body)
        except ApiException as e:
            if e.status != 409:
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
        if "AWS_ACCESS_KEY_ID" in os.environ:
            self.cloud_creds["aws"] = {
                "aws_access_key_id": base64.b64encode(
                    os.environ.get("AWS_ACCESS_KEY_ID").encode()
                ).decode("utf-8"),
                "aws_secret_access_key": base64.b64encode(
                    os.environ.get("AWS_SECRET_ACCESS_KEY").encode()
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

        # generate unique ID that conforms to K8s requirements (<63 alphanumeric chars, -, _)
        id = self.id_generator()

        # print("Listing pods with their IPs:")
        # ret = self.core_client.list_pod_for_all_namespaces(watch=False)
        # for i in ret.items:
        #    print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
        # for pod in self.core_client.list_namespaced_pod(self.ns).items:
        #    print("%s\t%s\t%s" % (pod.status.pod_ip, pod.metadata.namespace, pod.metadata.name))

        # create PVC for input data
        input_pvc_name = f"input-data-{id}"
        if self.use_nfs:
            input_pv_name = f"{input_pvc_name}-pv"
            body = client.V1PersistentVolume(
                metadata=client.V1ObjectMeta(name=input_pv_name),
                spec={
                    "accessModes": ["ReadWriteOnce", "ReadOnlyMany"],
                    "capacity": {"storage": "1Gi"},
                    "nfs": {"server": self.use_nfs,
                            "path": "/"},
                },
            )
            self.core_client.create_persistent_volume(
                body=body
            )
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=input_pvc_name),
                spec={
                    "accessModes": ["ReadWriteOnce", "ReadOnlyMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                    "volumeName": input_pv_name,
                    "storageClassName": "",
                },
            )
        else:
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=input_pvc_name),
                spec={
                    "accessModes": ["ReadWriteOnce", "ReadOnlyMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            )
        self.core_client.create_namespaced_persistent_volume_claim(
            namespace=self.ns, body=body
        )

        # create PVC for tmpout
        tmpout_pvc_name = f"tmpout-{id}"
        if self.use_nfs:
            tmpout_pv_name = f"{tmpout_pvc_name}-pv"
            body = client.V1PersistentVolume(
                metadata=client.V1ObjectMeta(name=tmpout_pv_name),
                spec={
                    "accessModes": ["ReadOnlyMany"],
                    "capacity": {"storage": "1Gi"},
                    "nfs": {"server": self.use_nfs,
                            "path": "/"},
                },
            )
            self.core_client.create_persistent_volume(
                body=body
            )
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=tmpout_pvc_name),
                spec={
                    "accessModes": ["ReadOnlyMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                    "volumeName": tmpout_pv_name,
                    "storageClassName": "",
                },
            )
        else:
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=tmpout_pvc_name),
                spec={
                    "accessModes": ["ReadWriteMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            )
        self.core_client.create_namespaced_persistent_volume_claim(
            namespace=self.ns, body=body
        )

        # create PVC for output data
        output_pvc_name = f"output-data-{id}"
        if self.use_nfs:
            output_pv_name = f"{output_pvc_name}-pv"
            body = client.V1PersistentVolume(
                metadata=client.V1ObjectMeta(name=output_pv_name),
                spec={
                    "accessModes": ["ReadOnlyMany"],
                    "capacity": {"storage": "1Gi"},
                    "nfs": {"server": self.use_nfs,
                            "path": "/"},
                },
            )
            self.core_client.create_persistent_volume(
                body=body
            )
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=output_pvc_name),
                spec={
                    "accessModes": ["ReadOnlyMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                    "volumeName": output_pv_name,
                    "storageClassName": "",
                },
            )
        else:
            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=output_pvc_name),
                spec={
                    "accessModes": ["ReadWriteMany"],
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            )
        self.core_client.create_namespaced_persistent_volume_claim(
            namespace=self.ns, body=body
        )

        # create job
        k8s_job_spec = {
            "template": {
                "spec": {
                    "containers": [ {
                            "name": "calrissian-job",
                            "image": "pymonger/calrissian:latest",
                            "imagePullPolicy": "Always",
                            "envFrom": [{"secretRef": {"name": "aws-creds"}}],
                            "command": ["calrissian"],
                            "args": [
                                "--debug",
                                "--stdout",
                                "/calrissian/output-data/docker-output.json",
                                "--stderr",
                                "/calrissian/output-data/docker-stderr.log",
                                "--max-ram",
                                "16G",
                                "--max-cores",
                                "8",
                                "--tmp-outdir-prefix",
                                "/calrissian/tmpout/",
                                "--outdir",
                                "/calrissian/output-data/",
                                "--usage-report",
                                "/calrissian/output-data/docker-usage.json",
                                job_spec["process"]["owsContextURL"],
                            ],
                            "volumeMounts": [
                                {
                                    "mountPath": "/calrissian/input-data",
                                    "name": input_pvc_name,
                                    "readOnly": True,
                                },
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
                            "name": input_pvc_name,
                            "persistentVolumeClaim": {
                                "claimName": input_pvc_name,
                                "readOnly": True,
                            },
                        },
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

        # add initContainer to allow read-write NFS mounted volumes
        if self.use_nfs:
            k8s_job_spec["template"]["spec"]["initContainers"] = [{
                "name": "init-nfs-volumes",
                "image": "busybox",
                "imagePullPolicy": "Always",
                "command": ["chmod"],
                "args": [
                    "-R",
                    "+t",
                    "/calrissian"
                ],
                "volumeMounts": [
                    {
                        "mountPath": "/calrissian/input-data",
                        "name": input_pvc_name,
                    },
                    {
                        "mountPath": "/calrissian/tmpout",
                        "name": tmpout_pvc_name,
                    },
                    {
                        "mountPath": "/calrissian/output-data",
                        "name": output_pvc_name,
                    },
                ]
            }]

            # remove readOnly specs
            del k8s_job_spec["template"]["spec"]["containers"][0]["volumeMounts"][0]["readOnly"]
            del k8s_job_spec["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["readOnly"]

        # populate input params
        for k, v in job_spec["inputs"].items():
            if v is None:
                k8s_job_spec["template"]["spec"]["containers"][0]["args"].append(
                    f"--{k}"
                )
                # TODO: need better way of detecting when to use secrets; for now hard coding
                # by looking for the string
                if "aws_access_key_id" in k:
                    k8s_job_spec["template"]["spec"]["containers"][0]["args"].append(
                        "$(aws_access_key_id)"
                    )
                elif "aws_secret_access_key" in k:
                    k8s_job_spec["template"]["spec"]["containers"][0]["args"].append(
                        "$(aws_secret_access_key)"
                    )
            else:
                k8s_job_spec["template"]["spec"]["containers"][0]["args"].extend(
                    [f"--{k}", f"{v}"]
                )
        job_id = f"calrissian-job-{id}"
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
            "k8s_input_pvc_name": input_pvc_name,
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
        #print(f"backend_info: {job_spec['backend_info']}")
        #print(f"type(backend_info): {type(job_spec['backend_info'])}")
        #print(f"job_spec: {json.dumps(job_spec, indent=2)}")
        k8s_job_id = job_spec["backend_info"]["k8s_job_id"]
        batch_api = client.BatchV1Api()
        api_response = batch_api.read_namespaced_job(
            name=k8s_job_id, namespace=self.ns, pretty=True
        ) 
        print(api_response.status)
        #api_response_sanitized = client.ApiClient().sanitize_for_serialization(api_response)
        #print(f"api_response_sanitized: {json.dumps(api_response_sanitized, indent=2)}")

        # determine K8s job state:
        # https://v1-19.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#job-v1-batch
        # and map to ADES job state:
        # https://raw.githubusercontent.com/opengeospatial/wps-rest-binding/master/core/openapi/schemas/statusCode.yaml
        conditions = api_response.status.conditions
        #print(f"condtions: {conditions}")
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
            if api_response.status.active > 0:
                job_spec["status"] = "running"
        else:
            raise NotImplemented(f"Unhandled condition type: {type(conditions)}")
        return job_spec

    def get_job_results(self, job_spec):
        res = {
            "links": [
                {
                    "href": "https://mypath",
                    "rel": "result",
                    "type": "application/json",
                    "title": "mytitle",
                }
            ]
        }
        return {**job_spec, **res}
