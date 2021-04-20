// Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package job

import (
	"context"
	"encoding/json"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/tosca"
)

// DDIToHPCExecution holds DDI to HPC data transfer job Execution properties
type DDIToHPCExecution struct {
	*DDIJobExecution
}

// Execute executes a synchronous operation
func (e *DDIToHPCExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		var locationName string
		locationName, err = e.SetLocationFromAssociatedHPCJob(ctx)
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Location for %s is %s", e.NodeName, locationName)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting data transfer request %q", e.NodeName)
		err = e.submitDataTransferRequest(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit data transfer for node %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		/*
			err = e.cancelJob(ctx)
			if err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
					"Failed to cancel Job %q, error %s", e.NodeName, err.Error())

			}
		*/
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)

	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}

func (e *DDIToHPCExecution) submitDataTransferRequest(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	sourcePath := e.GetValueFromEnvInputs(ddiDatasetPathEnvVar)
	if sourcePath == "" {
		return errors.Errorf("Failed to get path of dataset to transfer from DDI")
	}

	destPath := e.GetValueFromEnvInputs(hpcDirectoryPathEnvVar)
	if destPath == "" {
		return errors.Errorf("Failed to get HPC directory path")
	}

	serverFQDN := e.GetValueFromEnvInputs(hpcServerEnvVar)
	if serverFQDN == "" {
		return errors.Errorf("Failed to get HPC server")
	}

	res := strings.SplitN(serverFQDN, ".", 2)
	targetSystem := res[0] + "_home"

	heappeJobIDStr := e.GetValueFromEnvInputs(heappeJobIDEnvVar)
	if heappeJobIDStr == "" {
		return errors.Errorf("Failed to get ID of associated job")
	}
	heappeJobID, err := strconv.ParseInt(heappeJobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			heappeJobIDStr, e.DeploymentID, e.NodeName)
		return err
	}

	heappeURL := e.GetValueFromEnvInputs(heappeURLEnvVar)
	if heappeURL == "" {
		return errors.Errorf("Failed to get HEAppE URL of job %d", heappeJobID)
	}

	taskName := e.GetValueFromEnvInputs(taskNameEnvVar)
	if taskName == "" {
		return errors.Errorf("Failed to get task name")
	}

	strVal := e.GetValueFromEnvInputs(tasksNameIdEnvVar)
	if strVal == "" {
		return errors.Errorf("Failed to get map of tasks name-id from associated job")
	}
	var tasksNameID map[string]string
	err = json.Unmarshal([]byte(strVal), &tasksNameID)
	if err != nil {
		return errors.Wrapf(err, "Failed to unmarshall map od task name - task id %s", strVal)
	}

	taskIDStr, found := tasksNameID[taskName]
	if !found {
		return errors.Errorf("Failed to find task %s in associated job", taskName)
	}
	taskID, err := strconv.ParseInt(taskIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected task ID value %q for deployment %s node %s",
			taskIDStr, e.DeploymentID, e.NodeName)
		return err
	}
	taskDirPath := path.Join(destPath, taskIDStr)

	metadata, err := e.getMetadata(ctx)
	if err != nil {
		return err
	}

	requestID, err := ddiClient.SubmitDDIToHPCDataTransfer(metadata, e.Token, sourcePath, targetSystem, taskDirPath, heappeURL, heappeJobID, taskID)
	if err != nil {
		return err
	}

	// Store the request id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		requestIDConsulAttribute, requestID)
	if err != nil {
		err = errors.Wrapf(err, "Request %s submitted, but failed to store this request id", requestID)
	}
	return err
}
