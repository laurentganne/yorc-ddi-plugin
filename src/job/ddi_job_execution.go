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

	"github.com/laurentganne/yorc-ddi-plugin/v1/common"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	installOperation                  = "install"
	uninstallOperation                = "uninstall"
	requestIDConsulAttribute          = "request_id"
	ddiDatasetPathConsulAttribute     = "ddi_dataset_path"
	tokenEnvVar                       = "TOKEN"
	ddiDatasetPathEnvVar              = "DDI_DATASET_PATH"
	ddiPathEnvVar                     = "DDI_PATH"
	cloudStagingAreaDatasetPathEnvVar = "CLOUD_STAGING_AREA_DATASET_PATH"
	ipAddressEnvVar                   = "IP_ADDRESS"
	ddiToCloudCapability              = "ddi_to_cloud"
)

// DDIJobExecution holds DDI job Execution properties
type DDIJobExecution struct {
	*common.DDIExecution
	ActionType             string
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (e *DDIJobExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
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
	data["requestID"] = requestID
	data["token"] = token

	return &prov.Action{ActionType: e.ActionType, Data: data}, e.MonitoringTimeInterval, err
}

// ResolveExecution resolves inputs before the execution of an operation
func (e *DDIJobExecution) ResolveExecution(ctx context.Context) error {
	return e.resolveInputs(ctx)
}

func (e *DDIJobExecution) resolveInputs(ctx context.Context) error {
	var err error
	log.Debugf("Get environment inputs for node:%q", e.NodeName)
	e.EnvInputs, e.VarInputsNames, err = operations.ResolveInputsWithInstances(
		ctx, e.DeploymentID, e.NodeName, e.TaskID, e.Operation, nil, nil)
	log.Debugf("Environment inputs: %v", e.EnvInputs)
	return err
}

func (e *DDIJobExecution) getValueFromEnvInputs(envVar string) string {

	var result string
	for _, envInput := range e.EnvInputs {
		if envInput.Name == envVar {
			result = envInput.Value
			break
		}
	}
	return result

}

func (e *DDIJobExecution) getRequestID(ctx context.Context) (string, error) {

	val, err := deployments.GetInstanceAttributeValue(ctx, e.DeploymentID, e.NodeName, "0", requestIDConsulAttribute)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get request ID for deployment %s node %s", e.DeploymentID, e.NodeName)
	} else if val == nil {
		return "", errors.Errorf("Found no request id for deployment %s node %s", e.DeploymentID, e.NodeName)
	}

	return val.RawString(), err
}

func (e *DDIJobExecution) setCloudStagingAreaAccessDetails(ctx context.Context) error {

	ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	return e.SetCloudStagingAreaAccessCapabilityAttributes(ctx, ddiClient)
}

func getDDIClient(ctx context.Context, cfg config.Configuration, deploymentID, nodeName string) (ddi.Client, error) {
	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return nil, err
	}

	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx, deploymentID,
		nodeName, common.DDIInfrastructureType)
	if err != nil {
		return nil, err
	}

	return ddi.GetClient(locationProps)
}
