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

func CreateInstructionManual() (*RuntimeObjectDepencyYaml, error) {
	// Read Dependcy YAML File into Struct
	filepath := "/Users/hammer/go/src/github.com/blackducksoftware/kb-synopsys-operator/controllers/alert-dependencies.yaml"
	dependencyYamlBytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read from file %s: %s", filepath, err)
	}

	dependencyYamlStruct := &RuntimeObjectDepencyYaml{}
	err = yaml.Unmarshal(dependencyYamlBytes, dependencyYamlStruct)
	if err != nil {
		return nil, err
	}
	return dependencyYamlStruct, nil
}

func (r *AlertReconciler) ScheduleResources(alert alertsv1.Alert, req ctrl.Request, content []byte, instructionManual *RuntimeObjectDepencyYaml, ctx context.Context, log logr.Logger) error {
	mapOfKindToDesiredRuntimeObject, mapOfComponentLabelToDesiredRuntimeObject := r.convertYamlFileToRuntimeObjects(content)
	log.V(1).Info("Parsed the yaml into K8s runtime object", "mapOfKindToDesiredRuntimeObject", mapOfKindToDesiredRuntimeObject, "mapOfComponentLabelToDesiredRuntimeObject", mapOfComponentLabelToDesiredRuntimeObject)

	// TODO: potentially this is what convertYamlFileToRuntimeObjects returns
	mapOfKindToMapOfNameToDesiredRuntimeObject := make(map[string]map[string]runtime.Object, len(mapOfKindToDesiredRuntimeObject))
	for kind, listOfRuntimeObjects := range mapOfKindToDesiredRuntimeObject {
		for _, item := range listOfRuntimeObjects {
			key := item.(metav1.Object).GetName()
			// weird golang thing, have to initialize the nested map if it doesn't exist
			if _, ok := mapOfKindToMapOfNameToDesiredRuntimeObject[kind]; !ok {
				mapOfKindToMapOfNameToDesiredRuntimeObject[kind] = make(map[string]runtime.Object, 0)
			}
			mapOfKindToMapOfNameToDesiredRuntimeObject[kind][key] = item
		}
	}

	// Get current runtime objects "owned" by Alert CR
	fmt.Printf("Creating Tasks for RuntimeObjects...\n")
	var listOfCurrentRuntimeObjectsOwnedByAlertCr metav1.List
	if err := r.List(ctx, &listOfCurrentRuntimeObjectsOwnedByAlertCr, client.InNamespace(req.Namespace), client.MatchingField(jobOwnerKey, req.Name)); err != nil {
		log.Error(err, "unable to list currentRuntimeObjectsOwnedByAlertCr")
		//TODO: redo
		//return ctrl.Result{}, nil
	}

	// If any of the current objects are not in the desired objects, delete them
	fmt.Printf("Creating Task Dependencies...\n")
	for _, currentRuntimeObjectOwnedByAlertCr := range listOfCurrentRuntimeObjectsOwnedByAlertCr.Items {
		kind := currentRuntimeObjectOwnedByAlertCr.Object.GetObjectKind().GroupVersionKind().Kind
		uniqueIdentifierRuntimeObject := currentRuntimeObjectOwnedByAlertCr.Object.(runtime.Object)
		uniqueIdentifier := uniqueIdentifierRuntimeObject.(metav1.Object).GetName()
		_, ok := mapOfKindToMapOfNameToDesiredRuntimeObject[kind][uniqueIdentifier]
		if !ok {
			err := r.Delete(ctx, uniqueIdentifierRuntimeObject)
			if err != nil {
				// if any error in deleting, just continue
				continue
			}
		}
	}

	alertScheduler := scheduler.New(scheduler.ConcurrentTasks(5))
	taskMap := make(map[string]*scheduler.Task)
	for label, listOfRuntimeObjects := range mapOfComponentLabelToDesiredRuntimeObject {
		for _, runtimeObject := range listOfRuntimeObjects {
			taskFunc := func(ctx context.Context) error {
				_, err := r.EnsureRuntimeObjects(ctx, log, &alert, []runtime.Object{runtimeObject})
				return err
			}
			task := alertScheduler.AddTask(taskFunc)
			taskMap[label] = task
		}
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
	content, err := r.httpGet(alert.Spec.FinalYamlUrl)
	if err != nil {
		log.Error(err, "HTTPGet failed")
		return ctrl.Result{}, err
	}

	// 2. Create Instruction Manual From Runtime Objects
	instructionManual, err := CreateInstructionManual()
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Deploy Resources with Instruction Manual
	err = r.ScheduleResources(alert, req, content, instructionManual, ctx, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	// TODO: By adding sha, we no longer need to requeue after (awesome!!), but it's here just in case you need to re-enable it
	//return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	return ctrl.Result{}, nil
}

func (r *AlertReconciler) httpGet(url string) (content []byte, err error) {
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

func (r *AlertReconciler) EnsureRuntimeObjects(ctx context.Context, log logr.Logger, alert *alertsv1.Alert, listOfDesiredRuntimeObjects []runtime.Object) (ctrl.Result, error) {
	for _, desiredRuntimeObject := range listOfDesiredRuntimeObjects {

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
		//	if err := ctrl.SetControllerReference(alert, desiredRuntimeObject.(metav1.Object), r.Scheme); err != nil {
		//		// Requeue if we cannot set owner on the object
		//		//return err
		//		return nil
		//	}
		//	return nil
		//})

		// set an owner reference
		if err := ctrl.SetControllerReference(alert, desiredRuntimeObject.(metav1.Object), r.Scheme); err != nil {
			// requeue if we cannot set owner on the object
			// TODO: change this to requeue, and only not requeue when we get "newAlreadyOwnedError", i.e: if it's already owned by our CR
			//return ctrl.Result{}, err
			return ctrl.Result{}, nil
		}

		var opResult controllerutil.OperationResult
		key, err := client.ObjectKeyFromObject(desiredRuntimeObject)
		if err != nil {
			opResult = controllerutil.OperationResultNone
		}

		currentRuntimeObject := desiredRuntimeObject.DeepCopyObject()
		if err := r.Client.Get(ctx, key, currentRuntimeObject); err != nil {
			if !apierrs.IsNotFound(err) {
				opResult = controllerutil.OperationResultNone
			}
			if err := r.Client.Create(ctx, desiredRuntimeObject); err != nil {
				opResult = controllerutil.OperationResultNone
			}
			opResult = controllerutil.OperationResultCreated
		}

		existing := currentRuntimeObject
		if reflect.DeepEqual(existing, desiredRuntimeObject) {
			opResult = controllerutil.OperationResultNone
		}

		if err := r.Client.Update(ctx, desiredRuntimeObject); err != nil {
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

func (r *AlertReconciler) convertYamlFileToRuntimeObjects(fileR []byte) (map[string][]runtime.Object, map[string][]runtime.Object) {

	//acceptedK8sTypes := regexp.MustCompile(`(Role|ClusterRole|RoleBinding|ClusterRoleBinding|ServiceAccount|ConfigMap)`)
	fileAsString := string(fileR[:])
	listOfSingleK8sResourceYaml := strings.Split(fileAsString, "---")
	kindRuntimeObjectMap := make(map[string][]runtime.Object, 0)
	labelComponentRuntimeObjectMap := make(map[string][]runtime.Object, 0)

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
		kindRuntimeObjectMap[groupVersionKind.Kind] = append(kindRuntimeObjectMap[groupVersionKind.Kind], runtimeObject)
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
		// labelComponentRuntimeObjectMap[labels["component"]] = append(labelComponentRuntimeObjectMap[labels["component"]], runtimeObject)
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
		label := fmt.Sprintf("%s.%s.%s", runtimeObjectKind, runtimeObjectNamespace, runtimeObjectName)
		fmt.Printf("Creating RuntimeObject Label: %s\n", label)
		labelComponentRuntimeObjectMap[label] = append(labelComponentRuntimeObjectMap[label], runtimeObject)
		// ABOVE - New Unique Label Code
	}
	return kindRuntimeObjectMap, labelComponentRuntimeObjectMap
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
