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
	// built-in
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"gopkg.in/yaml.v2"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	// logr
	"github.com/go-logr/logr"

	// k8s api
	corev1 "k8s.io/api/core/v1"

	// apimachinery
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	// client-go
	"k8s.io/client-go/kubernetes/scheme"

	// controller-runtime
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	// controller specific imports
	alertsv1 "github.com/yashbhutwala/kb-synopsys-operator/api/v1"

	scheduler "github.com/yashbhutwala/go-scheduler"
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

type VertexInterface struct {
	Execute func() (ctrl.Result, error)
}

func (r *AlertReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("name and namespace of the alert to reconcile", req.NamespacedName)

	var alert alertsv1.Alert
	if err := r.Get(ctx, req.NamespacedName, &alert); err != nil {
		log.Error(err, "unable to fetch Alert")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, ignoreNotFound(err)
	}

	// 1. Get List of Runtime Objects (Base Yamls)
	// TODO: either read contents of yaml from locally mounted file
	// read content of full desired yaml from externally hosted file
	mapOfUniqueIdToDesiredRuntimeObject, err := r.getRuntimeObjectMaps(alert, log, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Create Instruction Manual From Runtime Objects
	inctructionManualLocation := "https://raw.githubusercontent.com/yashbhutwala/kb-synopsys-operator/master/controllers/alert-dependencies.yaml"
	instructionManual, err := CreateInstructionManual(inctructionManualLocation)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Deploy Resources with Instruction Manual
	err = ScheduleResources(r.Client, alert.GetObjectMeta(), mapOfUniqueIdToDesiredRuntimeObject, instructionManual)
	if err != nil {
		return ctrl.Result{}, err
	}

	// TODO: By adding sha, we no longer need to requeue after (awesome!!), but it's here just in case you need to re-enable it
	//return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	return ctrl.Result{}, nil
}

// +kubebuilder:rbac:groups=alerts.synopsys.com,resources=alerts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts.synopsys.com,resources=alerts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=alerts,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts,resources=pods/status,verbs=get
// +kubebuilder:rbac:groups=alerts,resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=alerts,resources=services,verbs=get;list;watch;create;update;patch;delete

// Dependency Resources from YAML file
type RuntimeObjectDependency struct {
	Obj           string `yaml:"obj"`
	IsDependentOn string `yaml:"isdependenton"`
}
type RuntimeObjectDepencyYaml struct {
	Groups       map[string][]string       `yaml:"runtimeobjectsgroupings"`
	Dependencies []RuntimeObjectDependency `yaml:"runtimeobjectdependencies"`
}

func CreateInstructionManual(instructionManualLocation string) (*RuntimeObjectDepencyYaml, error) {
	// Read Dependcy YAML File into Struct
	dependencyYamlBytes, err := httpGet(instructionManualLocation)
	if err != nil {
		return nil, err
	}

	dependencyYamlStruct := &RuntimeObjectDepencyYaml{}
	err = yaml.Unmarshal(dependencyYamlBytes, dependencyYamlStruct)
	if err != nil {
		return nil, err
	}
	return dependencyYamlStruct, nil
}

func (r *AlertReconciler) getRuntimeObjectMaps(alert alertsv1.Alert, log logr.Logger, myScheme *runtime.Scheme) (map[string]runtime.Object, error) {
	content, err := httpGet(alert.Spec.FinalYamlUrl)
	if err != nil {
		log.Error(err, "HTTPGet failed")
		return nil, err
	}

	mapOfUniqueIdToDesiredRuntimeObject := r.convertYamlFileToRuntimeObjects(content)
	log.V(1).Info("Parsed the yaml into K8s runtime object", "mapOfUniqueIdToDesiredRuntimeObject", mapOfUniqueIdToDesiredRuntimeObject)

	for _, desiredRuntimeObject := range mapOfUniqueIdToDesiredRuntimeObject {
		// set an owner reference
		if err := ctrl.SetControllerReference(&alert, desiredRuntimeObject.(metav1.Object), myScheme); err != nil {
			// requeue if we cannot set owner on the object
			// TODO: change this to requeue, and only not requeue when we get "newAlreadyOwnedError", i.e: if it's already owned by our CR
			//return ctrl.Result{}, err
			//return ctrl.Result{}, nil
			//return nil, err
		}
	}

	return mapOfUniqueIdToDesiredRuntimeObject, nil
}

func ScheduleResources(myClient client.Client, cr metav1.Object, mapOfUniqueIdToDesiredRuntimeObject map[string]runtime.Object, instructionManual *RuntimeObjectDepencyYaml) error {
	ctx := context.Background()
	log := ctrl.Log.WithName("MP/YB Library")
	// Get current runtime objects "owned" by Alert CR
	fmt.Printf("Creating Tasks for RuntimeObjects...\n")
	var listOfCurrentRuntimeObjectsOwnedByAlertCr metav1.List
	if err := myClient.List(ctx, &listOfCurrentRuntimeObjectsOwnedByAlertCr, client.InNamespace(cr.GetNamespace()), client.MatchingField(jobOwnerKey, cr.GetName())); err != nil {
		log.Error(err, "unable to list currentRuntimeObjectsOwnedByAlertCr")
		//TODO: redo
		//return ctrl.Result{}, nil
	}

	// If any of the current objects are not in the desired objects, delete them
	fmt.Printf("Creating Task Dependencies...\n")
	accessor := meta.NewAccessor()
	for _, currentRuntimeRawExtensionOwnedByAlertCr := range listOfCurrentRuntimeObjectsOwnedByAlertCr.Items {
		currentRuntimeObjectOwnedByAlertCr := currentRuntimeRawExtensionOwnedByAlertCr.Object.(runtime.Object)
		currentRuntimeObjectKind, _ := accessor.Kind(currentRuntimeObjectOwnedByAlertCr)
		currentRuntimeObjectName, _ := accessor.Name(currentRuntimeObjectOwnedByAlertCr)
		currentRuntimeObjectNamespace, _ := accessor.Namespace(currentRuntimeObjectOwnedByAlertCr)
		uniqueId := fmt.Sprintf("%s.%s.%s", currentRuntimeObjectKind, currentRuntimeObjectNamespace, currentRuntimeObjectName)
		_, ok := mapOfUniqueIdToDesiredRuntimeObject[uniqueId]
		if !ok {
			err := myClient.Delete(ctx, currentRuntimeObjectOwnedByAlertCr)
			if err != nil {
				// if any error in deleting, just continue
				continue
			}
		}
	}

	alertScheduler := scheduler.New(scheduler.ConcurrentTasks(5))
	taskMap := make(map[string]*scheduler.Task)
	for uniqueId, desiredRuntimeObject := range mapOfUniqueIdToDesiredRuntimeObject {
		rto := desiredRuntimeObject.DeepCopyObject()
		taskFunc := func(ctx context.Context) error {
			_, err := EnsureRuntimeObjects(myClient, ctx, log, rto)
			return err
		}
		task := alertScheduler.AddTask(taskFunc)
		taskMap[uniqueId] = task
	}

	for _, dependency := range instructionManual.Dependencies {
		depTail := dependency.Obj
		depHead := dependency.IsDependentOn // depTail --> depHead
		fmt.Printf("Creating Task Dependency: %s -> %s\n", depTail, depHead)
		// Get all RuntimeObjects for the Tail
		tailRuntimeObjectIDs, ok := instructionManual.Groups[depTail]
		if !ok { // no group due to single object name
			tailRuntimeObjectIDs = []string{depTail}
		}
		// Get all RuntimeObjects for the Head
		headRuntimeObjectIDs, ok := instructionManual.Groups[depHead]
		if !ok { // no group due to single object name
			headRuntimeObjectIDs = []string{depHead}
		}
		// Create dependencies from each tail to each head
		for _, tailRuntimeObjectName := range tailRuntimeObjectIDs {
			for _, headRuntimeObjectName := range headRuntimeObjectIDs {
				taskMap[tailRuntimeObjectName].DependsOn(taskMap[headRuntimeObjectName])
				fmt.Printf("   -  %s -> %s\n", tailRuntimeObjectName, headRuntimeObjectName)
			}
		}
	}

	if err := alertScheduler.Run(context.Background()); err != nil {
		return err
	}
	return nil
}

func httpGet(url string) (content []byte, err error) {
	response, err := http.Get(url)
	if err != nil {
		return
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("INVALID RESPONSE; status: %s", response.Status)
	}
	return ioutil.ReadAll(response.Body)
}

func EnsureRuntimeObjects(myClient client.Client, ctx context.Context, log logr.Logger, desiredRuntimeObject runtime.Object) (ctrl.Result, error) {
	// TODO: either get this working or wait for server side apply
	// TODO: https://github.com/kubernetes-sigs/controller-runtime/issues/347
	// TODO: https://github.com/kubernetes-sigs/controller-runtime/issues/464
	// TODO: https://godoc.org/sigs.k8s.io/controller-runtime/pkg/controller/controllerutil#CreateOrUpdate

	//pointerToDesiredRuntimeObject := &desiredRuntimeObject
	//copyOfDesiredRuntimeObject := desiredRuntimeObject.DeepCopyObject()
	//pointerToCopyOfDesiredRuntimeObject := &copyOfDesiredRuntimeObject
	//opResult, err := ctrl.CreateOrUpdate(ctx, r.Client, *pointerToDesiredRuntimeObject, func() error {
	//	*pointerToDesiredRuntimeObject = *pointerToCopyOfDesiredRuntimeObject
	//	// Set an owner reference
	//	if err := ctrl.SetControllerReference(cr, desiredRuntimeObject.(metav1.Object), r.Scheme); err != nil {
	//		// Requeue if we cannot set owner on the object
	//		//return err
	//		return nil
	//	}
	//	return nil
	//})

	var opResult controllerutil.OperationResult
	key, err := client.ObjectKeyFromObject(desiredRuntimeObject)
	if err != nil {
		opResult = controllerutil.OperationResultNone
	}

	currentRuntimeObject := desiredRuntimeObject.DeepCopyObject()
	if err := myClient.Get(ctx, key, currentRuntimeObject); err != nil {
		if !apierrs.IsNotFound(err) {
			opResult = controllerutil.OperationResultNone
		}
		if err := myClient.Create(ctx, desiredRuntimeObject); err != nil {
			opResult = controllerutil.OperationResultNone
		}
		opResult = controllerutil.OperationResultCreated
	}

	existing := currentRuntimeObject
	if reflect.DeepEqual(existing, desiredRuntimeObject) {
		opResult = controllerutil.OperationResultNone
	}

	if err := myClient.Update(ctx, desiredRuntimeObject); err != nil {
		opResult = controllerutil.OperationResultNone
	}

	log.V(1).Info("Result of CreateOrUpdate on CFSSL desiredRuntimeObject", "desiredRuntimeObject", desiredRuntimeObject, "opResult", opResult)

	// TODO: Case 1: we needed to update the configMap and now we should delete and redploy objects in STAGE 3, 4 ...
	// TODO: Case 2: we failed to update the configMap...TODO
	if err != nil {
		// TODO: delete everything in stages 3, 4 ... and requeue
		log.Error(err, "unable to create or update STAGE 2 objects, deleting all child Objects", "desiredRuntimeObject", desiredRuntimeObject)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// We generally want to ignore (not requeue) NotFound errors, since we’ll get a reconciliation request once the object exists, and requeuing in the meantime won’t help.
func ignoreNotFound(err error) error {
	if apierrs.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *AlertReconciler) convertYamlFileToRuntimeObjects(fileR []byte) map[string]runtime.Object {

	//acceptedK8sTypes := regexp.MustCompile(`(Role|ClusterRole|RoleBinding|ClusterRoleBinding|ServiceAccount|ConfigMap)`)
	fileAsString := string(fileR[:])
	listOfSingleK8sResourceYaml := strings.Split(fileAsString, "---")
	mapOfUniqueIdToDesiredRuntimeObject := make(map[string]runtime.Object, 0)

	for _, singleYaml := range listOfSingleK8sResourceYaml {
		if singleYaml == "\n" || singleYaml == "" {
			// ignore empty cases
			continue
		}
		decode := scheme.Codecs.UniversalDeserializer().Decode
		runtimeObject, groupVersionKind, err := decode([]byte(singleYaml), nil, nil)
		if err != nil {
			r.Log.V(1).Info("unable to decode a single yaml object, skipping", "singleYaml", singleYaml)
			continue
		}
		//if !acceptedK8sTypes.MatchString(groupVersionKind.Kind) {
		//	log.Printf("The custom-roles configMap contained K8s object types which are not supported! Skipping object with type: %s", groupVersionKind.Kind)
		//}

		accessor := meta.NewAccessor()
		// BELOW -- Old Label Code
		// labels, err := accessor.Labels(runtimeObject)
		// if err != nil {
		// 	r.Log.V(1).Info("unable to get labels for a k8s object, skipping", "runtimeObject", runtimeObject)
		// 	continue
		// }
		// mapOfUniqueIdToDesiredRuntimeObject[labels["component"]] = append(mapOfUniqueIdToDesiredRuntimeObject[labels["component"]], runtimeObject)
		// ABOVE -- Old Label Code
		// BELOW - New Unique Label Code
		runtimeObjectKind := groupVersionKind.Kind
		runtimeObjectName, err := accessor.Name(runtimeObject)
		if err != nil {
			fmt.Printf("Failed to get runtimeObject's name: %s", err)
			continue
		}
		runtimeObjectNamespace, err := accessor.Namespace(runtimeObject)
		if err != nil {
			fmt.Printf("Failed to get runtimeObject's namespace: %s", err)
			continue
		}
		uniqueId := fmt.Sprintf("%s.%s.%s", runtimeObjectKind, runtimeObjectNamespace, runtimeObjectName)
		fmt.Printf("Creating RuntimeObject Label: %s\n", uniqueId)
		mapOfUniqueIdToDesiredRuntimeObject[uniqueId] = runtimeObject
		// ABOVE - New Unique Label Code
	}
	return mapOfUniqueIdToDesiredRuntimeObject
}

func (r *AlertReconciler) SetIndexingForChildrenObjects(mgr ctrl.Manager, ro runtime.Object) error {
	if err := mgr.GetFieldIndexer().IndexField(ro, jobOwnerKey, func(rawObj runtime.Object) []string {
		// grab the job object, extract the owner...
		owner := metav1.GetControllerOf(ro.(metav1.Object))
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
	return nil
}

func (r *AlertReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Code here allows to kick off a reconciliation when objects our controller manages are changed somehow
	r.SetIndexingForChildrenObjects(mgr, &corev1.ConfigMap{})
	r.SetIndexingForChildrenObjects(mgr, &corev1.Service{})
	r.SetIndexingForChildrenObjects(mgr, &corev1.ReplicationController{})
	r.SetIndexingForChildrenObjects(mgr, &corev1.Secret{})

	alertBuilder := ctrl.NewControllerManagedBy(mgr).For(&alertsv1.Alert{})
	alertBuilder = alertBuilder.Owns(&corev1.ConfigMap{})
	alertBuilder = alertBuilder.Owns(&corev1.Service{})
	alertBuilder = alertBuilder.Owns(&corev1.ReplicationController{})
	alertBuilder = alertBuilder.Owns(&corev1.Secret{})

	return alertBuilder.Complete(r)
}
