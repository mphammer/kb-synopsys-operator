/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"log"
	//"reflect"

	//"regexp"
	"strings"

	"github.com/go-logr/logr"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	//"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	//"k8s.io/apimachinery/pkg/runtime/serializer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	alertsv1 "github.com/yashbhutwala/kb-alert-controller/api/v1"
)

// AlertReconciler reconciles a Alert object
type AlertReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = alertsv1.GroupVersion.String()
)

// We generally want to ignore (not requeue) NotFound errors, since we’ll get a reconciliation request once the object exists, and requeuing in the meantime won’t help.
func ignoreNotFound(err error) error {
	if apierrs.IsNotFound(err) {
		return nil
	}
	return err
}

func parseK8sYaml(fileR []byte) []runtime.Object {

	fmt.Printf("inside parseK8sYaml, input: %+v\n", fileR)

	//acceptedK8sTypes := regexp.MustCompile(`(Role|ClusterRole|RoleBinding|ClusterRoleBinding|ServiceAccount|ConfigMap)`)
	fileAsString := string(fileR[:])
	//fmt.Printf("inside parseK8sYaml, fileAsString: %+v\n", fileAsString)
	sepYamlfiles := strings.Split(fileAsString, "---")
	//fmt.Printf("inside parseK8sYaml, sepYamlfiles: %+v\n", sepYamlfiles)

	retVal := make([]runtime.Object, 0, len(sepYamlfiles))

	for _, f := range sepYamlfiles {

		//fmt.Printf("inside parseK8sYaml, f: %+v\n", f)
		if f == "\n" || f == "" {
			// ignore empty cases
			continue
		}

		decode := scheme.Codecs.UniversalDeserializer().Decode
		obj, _, err := decode([]byte(f), nil, nil)
		// DEBUG
		//obj, groupVersionKind, err := decode([]byte(f), nil, nil)
		//fmt.Printf("obj: %+v, gvk: %+s, err: %+v\n", obj, groupVersionKind, err)
		//fmt.Printf("tyoe of gvk: %+v", reflect.TypeOf(groupVersionKind))

		if err != nil {
			log.Println(fmt.Sprintf("Error while decoding YAML object. Err was: %s", err))
			continue
		}

		retVal = append(retVal, obj)

		//if !acceptedK8sTypes.MatchString(groupVersionKind.Kind) {
		//	log.Printf("The custom-roles configMap contained K8s object types which are not supported! Skipping object with type: %s", groupVersionKind.Kind)
		//} else {
		//retVal = append(retVal, obj)
		//}
	}
	return retVal
}

// +kubebuilder:rbac:groups=alerts.synopsys.com,resources=alerts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts.synopsys.com,resources=alerts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=alerts,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts,resources=pods/status,verbs=get
// +kubebuilder:rbac:groups=alerts,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts,resources=services,verbs=get;list;watch;create;update;patch;delete

func (r *AlertReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {

	ctx := context.Background()
	log := r.Log.WithValues("alert", req.NamespacedName)

	// your logic here
	var alert alertsv1.Alert
	if err := r.Get(ctx, req.NamespacedName, &alert); err != nil {
		log.Error(err, "unable to fetch Alert")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, ignoreNotFound(err)
	}

	yamlConfig := alert.Spec.YamlConfig
	log.V(1).Info("Got the yamlConfig", "job", yamlConfig)

	arrOfRuntimeObjs := parseK8sYaml([]byte(yamlConfig))
	log.V(1).Info("Parsed the yaml into K8s runtime object", "arrOfRuntimeObjs", arrOfRuntimeObjs)

	// STAGE 1: configMap
	fmt.Println("STAGE 1 I CHOOSE YOU!")
	count := 0
	for _, runtimeObj := range arrOfRuntimeObjs {
		// Set an owner reference
		if err := ctrl.SetControllerReference(&alert, runtimeObj.(metav1.Object), r.Scheme); err != nil {
			return ctrl.Result{}, nil
		}

		switch runtimeObj.(type) {
		case *corev1.ConfigMap:
			_, err := ctrl.CreateOrUpdate(ctx, r.Client, runtimeObj, func() error {
				return nil
			})
			if err != nil {
				log.Error(err, "unable to create runtimeObj", "runtimeObj", runtimeObj)
				return ctrl.Result{}, err
			}
			arrOfRuntimeObjs = append(arrOfRuntimeObjs[:count], arrOfRuntimeObjs[count+1:]...)
		default:
			count += 1
		}
	}

	// STAGE 2: cfssl
	fmt.Println("STAGE 2 Deploy!")
	count = 0
	for _, runtimeObj := range arrOfRuntimeObjs {
		// Set an owner reference
		if err := ctrl.SetControllerReference(&alert, runtimeObj.(metav1.Object), r.Scheme); err != nil {
			return ctrl.Result{}, nil
		}

		accessor := meta.NewAccessor()
		labels, _ := accessor.Labels(runtimeObj)
		fmt.Printf("labels: %+v \n", labels)
		fmt.Printf("labels[app]: %+v \n", labels["component"])

		switch strings.TrimSpace(labels["component"]) {
		case "cfssl":
			log.V(1).Info("About to make some dope K8s runtime object", "runtimeObj", runtimeObj)
			_, err := ctrl.CreateOrUpdate(ctx, r.Client, runtimeObj, func() error {
				return nil
			})
			if err != nil {
				log.Error(err, "unable to create runtimeObj", "runtimeObj", runtimeObj)
				return ctrl.Result{}, err
			}
			arrOfRuntimeObjs = append(arrOfRuntimeObjs[:count], arrOfRuntimeObjs[count+1:]...)
		default:
			count += 1
		}
	}

	// STAGE 3: alert
	fmt.Println("STAGE 3 Get in here!")
	count = 0
	for _, runtimeObj := range arrOfRuntimeObjs {
		// Set an owner reference
		if err := ctrl.SetControllerReference(&alert, runtimeObj.(metav1.Object), r.Scheme); err != nil {
			return ctrl.Result{}, nil
		}

		accessor := meta.NewAccessor()
		labels, _ := accessor.Labels(runtimeObj)
		fmt.Printf("labels: %+v \n", labels)
		fmt.Printf("labels[app]: %+v \n", labels["component"])

		switch strings.TrimSpace(labels["component"]) {
		case "alert":
			log.V(1).Info("About to make some dope K8s runtime object", "runtimeObj", runtimeObj)
			_, err := ctrl.CreateOrUpdate(ctx, r.Client, runtimeObj, func() error {
				return nil
			})
			if err != nil {
				log.Error(err, "unable to create runtimeObj", "runtimeObj", runtimeObj)
				return ctrl.Result{}, err
			}
			arrOfRuntimeObjs = append(arrOfRuntimeObjs[:count], arrOfRuntimeObjs[count+1:]...)
		default:
			count += 1
		}
	}

	// STAGE 4: everything else
	fmt.Println("STAGE 4 Gimme some more")
	count = 0
	for _, runtimeObj := range arrOfRuntimeObjs {
		// Set an owner reference
		if err := ctrl.SetControllerReference(&alert, runtimeObj.(metav1.Object), r.Scheme); err != nil {
			return ctrl.Result{}, nil
		}

		log.V(1).Info("About to make some dope K8s runtime object", "runtimeObj", runtimeObj)
		_, err := ctrl.CreateOrUpdate(ctx, r.Client, runtimeObj, func() error {
			return nil
		})
		if err != nil {
			log.Error(err, "unable to create runtimeObj", "runtimeObj", runtimeObj)
			return ctrl.Result{}, err
		}
		// TODO: do a cleanup of the remaining resources
	}

	//2a: list all child jobs in this namespace that belong to this Alert
	//var childConfigMapList corev1.ConfigMapList
	//if err := r.List(ctx, &childConfigMapList, client.InNamespace(req.Namespace), client.MatchingField(jobOwnerKey, req.Name)); err != nil {
	//	log.Error(err, "unable to list child Jobs")
	//	return ctrl.Result{}, err
	//}

	//for _, configMap := range desiredConfigMapList.Items {
	//	log.V(1).Info("About to make some dope K8s runtime object", "runtimeObj", configMap)
	//
	//	ctrl.CreateOrUpdate(ctx, r.Client, configMap, func() error {
	//		return nil
	//	})
	//}

	//var childServiceList corev1.ServiceList
	//if err := r.List(ctx, &childConfigMaps, client.InNamespace(req.Namespace), client.MatchingField(jobOwnerKey, req.Name)); err != nil {
	//	log.Error(err, "unable to list child Jobs")
	//	return ctrl.Result{}, err
	//}

	//for i, runtimeObj := range arrOfRuntimeObjs {
	//	//fmt.Printf(" inside the loop runtimeObj: %+v\n", runtimeObj)
	//	//fmt.Printf(" inside the loop runtimeObj: %+v\n", runtimeObj.(type))
	//	switch runtimeObj.(type) {
	//	case *corev1.ConfigMap:
	//		log.V(1).Info("About to make some dope K8s runtime object", "runtimeObj", runtimeObj)
	//
	//		//accessor, _ := conversion.EnforcePtr(runtimeObj)
	//		//var metav1Object = accessor.FieldByName("ObjectMeta")
	//		//
	//		//// Set an owner reference
	//		//if err := ctrl.SetControllerReference(accessor, metav1Object, r.Scheme); err != nil {
	//		//	fmt.Printf("I FAILED TO SET OWNER REFERENCE %+v \n", runtimeObjAccessor)
	//		//	return ctrl.Result{}, nil
	//		//}
	//
	//		//alertAccessor, _ := meta.Accessor(alert)
	//		//fmt.Printf("alertAccessor: %+v\n", alertAccessor)
	//		//runtimeObjAccessor, _ := meta.Accessor(runtimeObj)
	//		//fmt.Printf("runtimeObjAccessor: %+v\n", runtimeObjAccessor)
	//
	//		// alert.DeepCopyObject().(metav1.Object)
	//
	//		//fmt.Printf("alert before: %+v\n", alert)
	//
	//		//pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{alert.Gr})
	//
	//		//fmt.Printf("printing Alert CR: %+v", alert)
	//
	//		// Set an owner reference
	//		if err := ctrl.SetControllerReference(&alert, runtimeObj.(metav1.Object), r.Scheme); err != nil {
	//			return ctrl.Result{}, nil
	//		}
	//
	//		//var ns corev1.Namespace
	//		//var s conversion.Scope
	//		//
	//		//_ = runtime.Convert_runtime_Object_To_runtime_RawExtension(&runtimeObj, &ns, s)
	//
	//		//ns, _ := runtime.ObjectCreater(runtimeObj)
	//
	//		//ns := runtimeObj.(*corev1.Namespace)
	//
	//		if err := r.Create(ctx, runtimeObj); err != nil {
	//			log.Error(err, "unable to create runtimeObj", "runtimeObj", runtimeObj)
	//			return ctrl.Result{}, err
	//		}
	//
	//		arrOfRuntimeObjs = append(arrOfRuntimeObjs[:i], arrOfRuntimeObjs[i+1:]...)
	//	}
	//}

	return ctrl.Result{}, nil
}

//func setIndexing(mgr ctrl.Manager, ro metav1.Object) error {
//	if err := mgr.GetFieldIndexer().IndexField(ro.(runtime.Object), jobOwnerKey, func(rawObj runtime.Object) []string {
//		// grab the job object, extract the owner...
//		//configMap := rawObj.(*corev1.ConfigMap)
//		owner := metav1.GetControllerOf(ro)
//		if owner == nil {
//			return nil
//		}
//		// ...make sure it's a Alert...
//		if owner.APIVersion != apiGVStr || owner.Kind != "Alert" {
//			return nil
//		}
//
//		// ...and if so, return it
//		return []string{owner.Name}
//	}); err != nil {
//		return err
//	}
//	return nil
//}

func (r *AlertReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(&corev1.ConfigMap{}, jobOwnerKey, func(rawObj runtime.Object) []string {
		// grab the job object, extract the owner...
		namespace := rawObj.(*corev1.ConfigMap)
		owner := metav1.GetControllerOf(namespace)
		if owner == nil {
			return nil
		}
		// ...make sure it's a Alert...
		if owner.APIVersion != apiGVStr || owner.Kind != "Alert" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	if err := mgr.GetFieldIndexer().IndexField(&corev1.Service{}, jobOwnerKey, func(rawObj runtime.Object) []string {
		// grab the job object, extract the owner...
		namespace := rawObj.(*corev1.Service)
		owner := metav1.GetControllerOf(namespace)
		if owner == nil {
			return nil
		}
		// ...make sure it's a Alert...
		if owner.APIVersion != apiGVStr || owner.Kind != "Alert" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	if err := mgr.GetFieldIndexer().IndexField(&corev1.ReplicationController{}, jobOwnerKey, func(rawObj runtime.Object) []string {
		// grab the job object, extract the owner...
		namespace := rawObj.(*corev1.ReplicationController)
		owner := metav1.GetControllerOf(namespace)
		if owner == nil {
			return nil
		}
		// ...make sure it's a Alert...
		if owner.APIVersion != apiGVStr || owner.Kind != "Alert" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	if err := mgr.GetFieldIndexer().IndexField(&corev1.Secret{}, jobOwnerKey, func(rawObj runtime.Object) []string {
		// grab the job object, extract the owner...
		namespace := rawObj.(*corev1.Secret)
		owner := metav1.GetControllerOf(namespace)
		if owner == nil {
			return nil
		}
		// ...make sure it's a Alert...
		if owner.APIVersion != apiGVStr || owner.Kind != "Alert" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	alertBuilder := ctrl.NewControllerManagedBy(mgr).For(&alertsv1.Alert{})
	alertBuilder = alertBuilder.Owns(&corev1.ConfigMap{})
	alertBuilder = alertBuilder.Owns(&corev1.Service{})
	alertBuilder = alertBuilder.Owns(&corev1.ReplicationController{})
	alertBuilder = alertBuilder.Owns(&corev1.Secret{})

	return alertBuilder.Complete(r)
}
