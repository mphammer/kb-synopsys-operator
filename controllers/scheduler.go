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
	"fmt"
	"reflect"

	"context"

	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctrl "sigs.k8s.io/controller-runtime"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	scheduler "github.com/yashbhutwala/go-scheduler"

	alertsv1 "github.com/yashbhutwala/kb-synopsys-operator/api/v1"
)

func ScheduleResources(cr alertsv1.Alert, req ctrl.Request, k8sClient client.Client, Scheme *runtime.Scheme, mapOfUniqueIdToDesiredRuntimeObject map[string]runtime.Object, instructionManual *RuntimeObjectDepencyYaml, ctx context.Context) error {
	// Get current runtime objects "owned" by Alert CR
	var listOfCurrentRuntimeObjectsOwnedByAlertCr metav1.List
	if err := k8sClient.List(ctx, &listOfCurrentRuntimeObjectsOwnedByAlertCr, client.InNamespace(req.Namespace), client.MatchingField(jobOwnerKey, req.Name)); err != nil {
		//TODO: redo
		//return ctrl.Result{}, nil
	}

	// If any of the current objects are not in the desired objects, delete them
	accessor := meta.NewAccessor()
	fmt.Printf("Deleting extra objects...\n")
	for _, currentRuntimeRawExtensionOwnedByAlertCr := range listOfCurrentRuntimeObjectsOwnedByAlertCr.Items {
		currentRuntimeObjectOwnedByAlertCr := currentRuntimeRawExtensionOwnedByAlertCr.Object.(runtime.Object)
		currentRuntimeObjectKind, _ := accessor.Kind(currentRuntimeObjectOwnedByAlertCr)
		currentRuntimeObjectName, _ := accessor.Name(currentRuntimeObjectOwnedByAlertCr)
		currentRuntimeObjectNamespace, _ := accessor.Namespace(currentRuntimeObjectOwnedByAlertCr)
		uniqueId := fmt.Sprintf("%s.%s.%s", currentRuntimeObjectKind, currentRuntimeObjectNamespace, currentRuntimeObjectName)
		fmt.Printf("ObjectID: %+v\n", uniqueId)
		_, ok := mapOfUniqueIdToDesiredRuntimeObject[uniqueId]
		if !ok {
			err := k8sClient.Delete(ctx, currentRuntimeObjectOwnedByAlertCr)
			if err != nil {
				// if any error in deleting, just continue
				continue
			}
		}
	}

	alertScheduler := scheduler.New(scheduler.ConcurrentTasks(5))
	taskMap := make(map[string]*scheduler.Task)
	for label, runtimeObject := range mapOfUniqueIdToDesiredRuntimeObject {
		fmt.Printf("Creating Task for %s - %+v\n", label, runtimeObject.GetObjectKind())
		rto := runtimeObject.DeepCopyObject()
		taskMap[label] = alertScheduler.AddTask(func(ctx context.Context) error {
			fmt.Printf(" Inside Task Func - RTO: %+v\n", rto.GetObjectKind())
			err := EnsureRuntimeObjects(&cr, k8sClient, Scheme, rto, ctx)
			return err
		})
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

	// fmt.Printf(" - Alert Scheduler Task Count: %+v\n", alertScheduler.TaskCount())
	// fmt.Printf(" - Alert Scheduler Tasks: %+v\n", alertScheduler.Tasks())

	if err := alertScheduler.Run(ctx); err != nil {
		return err
	}
	return nil

}

func EnsureRuntimeObjects(cr *alertsv1.Alert, k8sclient client.Client, Scheme *runtime.Scheme, runtimeObject runtime.Object, ctx context.Context) error {
	fmt.Printf("Inside EnsureRuntimeObjects RTO: %+v...\n", runtimeObject.GetObjectKind())
	//fmt.Printf("Inside EnsureRuntimeObjects Scheme: %+v...\n", Scheme)
	fmt.Printf("Inside EnsureRuntimeObjects CR: %+v...\n", cr)
	// TODO: either get this working or wait for server side apply
	// TODO: https://github.com/kubernetes-sigs/controller-runtime/issues/347
	// TODO: https://github.com/kubernetes-sigs/controller-runtime/issues/464
	// TODO: https://godoc.org/sigs.k8s.io/controller-runtime/pkg/controller/controllerutil#CreateOrUpdate

	// TODO: Make setting an owner reference not use "ctrl api"
	if err := ctrl.SetControllerReference(cr, runtimeObject.(metav1.Object), Scheme); err != nil {
		// requeue if we cannot set owner on the object
		// TODO: change this to requeue, and only not requeue when we get "newAlreadyOwnedError", i.e: if it's already owned by our CR
		//return ctrl.Result{}, err
		return nil
	}

	// TODO: Borrowed from CreateOrUpdate - change to use same code base
	var opResult controllerutil.OperationResult
	key, err := client.ObjectKeyFromObject(runtimeObject)
	if err != nil {
		opResult = controllerutil.OperationResultNone
	}

	currentRuntimeObject := runtimeObject.DeepCopyObject()
	if err := k8sclient.Get(ctx, key, currentRuntimeObject); err != nil {
		if !apierrs.IsNotFound(err) {
			opResult = controllerutil.OperationResultNone
		}
		if err := k8sclient.Create(ctx, runtimeObject); err != nil {
			opResult = controllerutil.OperationResultNone
		}
		opResult = controllerutil.OperationResultCreated
	}

	existing := currentRuntimeObject
	if reflect.DeepEqual(existing, runtimeObject) {
		opResult = controllerutil.OperationResultNone
	}

	if err := k8sclient.Update(ctx, runtimeObject); err != nil {
		opResult = controllerutil.OperationResultNone
	}

	// TODO: Case 1: we needed to update the configMap and now we should delete and redploy objects in STAGE 3, 4 ...
	// TODO: Case 2: we failed to update the configMap...TODO
	if err != nil {
		// TODO: delete everything in stages 3, 4 ... and requeue
		return err
	}
	fmt.Printf("Runtime Obect was %+v\n\n", opResult)
	return nil
}
