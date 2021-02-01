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
	"strconv"
	"strings"
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/v1/common"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	installOperation                      = "install"
	uninstallOperation                    = "uninstall"
	metadataProperty                      = "metadata"
	requestIDConsulAttribute              = "request_id"
	destinationDatasetPathConsulAttribute = "destination_path"
	stagingAreaPathConsulAttribute        = "staging_area_directory_path"
	datasetPathConsulAttribute            = "dataset_path"
	datasetIDConsulAttribute              = "dataset_id"
	datasetFilesConsulAttribute           = "dataset_file_paths"
	fileNameConsulAttribute               = "file_name"
	ddiDatasetPathEnvVar                  = "DDI_DATASET_PATH"
	ddiDatasetFilePathsEnvVar             = "DDI_DATASET_FILE_PATHS"
	ddiPathEnvVar                         = "DDI_PATH"
	sourceSubDirEnvVar                    = "SOURCE_SUBDIRECTORY"
	sourceFileNameEnvVar                  = "SOURCE_FILE_NAME"
	cloudStagingAreaDatasetPathEnvVar     = "CLOUD_STAGING_AREA_DIRECTORY_PATH"
	timestampCloudStagingAreaDirEnvVar    = "TIMESTAMP_CLOUD_STAGING_AREA_DIRECTORY"
	hpcDirectoryPathEnvVar                = "HPC_DIRECTORY_PATH"
	hpcServerEnvVar                       = "HPC_SERVER"
	heappeJobIDEnvVar                     = "HEAPPE_JOB_ID"
	tasksNameIdEnvVar                     = "TASKS_NAME_ID"
	taskNameEnvVar                        = "TASK_NAME"
	ipAddressEnvVar                       = "IP_ADDRESS"
	filePatternEnvVar                     = "FILE_PATTERN"
	dataTransferCapability                = "data_transfer"
	datasetFilesProviderCapability        = "dataset_files"
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

	data := make(map[string]string)
	data[actionDataTaskID] = e.TaskID
	data[actionDataNodeName] = e.NodeName
	data[actionDataRequestID] = requestID
	data[actionDataToken] = e.Token

	return &prov.Action{ActionType: e.ActionType, Data: data}, e.MonitoringTimeInterval, err
}

func (e *DDIJobExecution) getMetadata(ctx context.Context) (ddi.Metadata, error) {
	var metadata ddi.Metadata

	val, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, metadataProperty)
	if err != nil {
		return metadata, err
	}
	if val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &metadata)
		if err != nil {
			return metadata, err
		}
	}

	// Set the publication yer if not set
	if metadata.PublicationYear == "" {
		metadata.PublicationYear = strconv.Itoa(time.Now().Year())
	}

	return metadata, err
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
