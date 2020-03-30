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
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/tosca"
)

// DeleteCloudDataExecution holds Cloud data deletion job Execution properties
type DeleteCloudDataExecution struct {
	KV                     *api.KV
	Cfg                    config.Configuration
	DeploymentID           string
	TaskID                 string
	NodeName               string
	Operation              prov.Operation
	EnvInputs              []*operations.EnvInput
	VarInputsNames         []string
	RequestID              string
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (e *DeleteCloudDataExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	if strings.ToLower(e.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", e.Operation.Name)
	}

	requestID, err := e.getRequestID(ctx)
	if err != nil {
		return nil, 0, err
	}

	token := e.getValueFromEnvInputs(tokenEnvVar)
	if token == "" {
		return nil, 0, errors.Errorf("Failed to get token")
	}

	data := make(map[string]string)
	data["taskID"] = e.TaskID
	data["nodeName"] = e.NodeName
	data["token"] = token
	data["requestID"] = requestID
	data["token"] = token

	return &prov.Action{ActionType: CloudDataDeleteAction, Data: data}, e.MonitoringTimeInterval, err
}

// Execute executes a synchronous operation
func (e *DeleteCloudDataExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		// Nothing to do here
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting Cloud data deletion request %q", e.NodeName)
		err = e.SubmitCloudStagingAreaDataDeletion(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit Cloud data deletion for node %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling request %q", e.NodeName)
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

// ResolveExecution resolves inputs before the execution of an operation
func (e *DeleteCloudDataExecution) ResolveExecution(ctx context.Context) error {
	return e.resolveInputs(ctx)
}

func (e *DeleteCloudDataExecution) resolveInputs(ctx context.Context) error {
	var err error
	log.Debugf("Get environment inputs for node:%q", e.NodeName)
	e.EnvInputs, e.VarInputsNames, err = operations.ResolveInputsWithInstances(
		ctx, e.DeploymentID, e.NodeName, e.TaskID, e.Operation, nil, nil)
	log.Debugf("Environment inputs: %v", e.EnvInputs)
	return err
}

func (e *DeleteCloudDataExecution) getValueFromEnvInputs(envVar string) string {

	var result string
	for _, envInput := range e.EnvInputs {
		if envInput.Name == envVar {
			result = envInput.Value
			break
		}
	}
	return result

}

// SubmitCloudStagingAreaDataDeletion deletes a dataset from the Cloud staging area
func (e *DeleteCloudDataExecution) SubmitCloudStagingAreaDataDeletion(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	token := e.getValueFromEnvInputs(tokenEnvVar)
	if token == "" {
		return errors.Errorf("Failed to get token")
	}

	dataPath := e.getValueFromEnvInputs(cloudStagingAreaDatasetPathEnvVar)
	if dataPath == "" {
		return errors.Errorf("Failed to get path of dataset to delete from Cloud storage")
	}

	requestID, err := ddiClient.SubmitCloudStagingAreaDataDeletion(token, dataPath)
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

func (e *DeleteCloudDataExecution) getRequestID(ctx context.Context) (string, error) {

	val, err := deployments.GetInstanceAttributeValue(ctx, e.DeploymentID, e.NodeName, "0", requestIDConsulAttribute)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get request ID for deployment %s node %s", e.DeploymentID, e.NodeName)
	} else if val == nil {
		return "", errors.Errorf("Found no request id for deployment %s node %s", e.DeploymentID, e.NodeName)
	}

	return val.RawString(), err
}
