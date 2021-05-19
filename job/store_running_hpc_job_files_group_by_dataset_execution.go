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

	"github.com/laurentganne/yorc-ddi-plugin/common"
	"github.com/laurentganne/yorc-ddi-plugin/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

// StoreRunningHPCJobFilesGroupByDataset holds DDI to HPC data transfer job Execution properties,
// and allows to group files to store in datasets according to a pattern
type StoreRunningHPCJobFilesGroupByDataset struct {
	*common.DDIExecution
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (e *StoreRunningHPCJobFilesGroupByDataset) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	if strings.ToLower(e.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", e.Operation.Name)
	}

	// Optional name of the task for which to get results
	val, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, taskNameProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", taskNameProperty, e.DeploymentID, e.NodeName)
	}
	var taskName string
	if val != nil && val.RawString() != "" {
		taskName = val.RawString()
	}

	// Optional list of file patterns
	val, err = deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, filesPatternProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", filesPatternProperty, e.DeploymentID, e.NodeName)
	}
	var filePatterns []string
	var filesPatternsStr string
	if val != nil && val.RawString() != "" {
		filesPatternsStr = val.RawString()
		err = json.Unmarshal([]byte(filesPatternsStr), &filePatterns)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Failed to parse %s property for deployment %s node %s, value %s",
				filesPatternProperty, e.DeploymentID, e.NodeName, filesPatternsStr)
		}
	}

	// Elapsed time in minutes since last modification after which a file can be stored in DDI
	val, err = deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, elapsedTimeMinutesProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", elapsedTimeMinutesProperty, e.DeploymentID, e.NodeName)
	}
	var elapsedTimeStr string
	if val != nil && val.RawString() != "" {
		elapsedTimeStr = val.RawString()
		// Checking the value is an int
		_, err := strconv.Atoi(val.RawString())
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Failed to parse int property %s for deployment %s node %s, value %s",
				elapsedTimeMinutesProperty, e.DeploymentID, e.NodeName, val.RawString())
		}
	}

	// Pattern used to group files by dataset
	val, err = deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, groupFilesPatternProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", groupFilesPatternProperty, e.DeploymentID, e.NodeName)
	}
	var groupFilesPattern string
	if val != nil && val.RawString() != "" {
		groupFilesPattern = val.RawString()
	}

	// Get encryption/compression settings
	encrypt := "no"
	encryption, err := deployments.GetBooleanNodeProperty(ctx, e.DeploymentID, e.NodeName, encryptProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", encryptProperty, e.DeploymentID, e.NodeName)
	}
	if encryption {
		encrypt = "yes"
	}
	compress := "no"
	compression, err := deployments.GetBooleanNodeProperty(ctx, e.DeploymentID, e.NodeName, compressProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", compressProperty, e.DeploymentID, e.NodeName)
	}
	if compression {
		compress = "yes"
	}

	// List of sites where to replicate datasets
	val, err = deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, replicationSitesProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", replicationSitesProperty, e.DeploymentID, e.NodeName)
	}
	var replicationSites []string
	var replicationSitesStr string
	if val != nil && val.RawString() != "" {
		replicationSitesStr = val.RawString()
		err = json.Unmarshal([]byte(replicationSitesStr), &replicationSites)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Failed to parse %s property for deployment %s node %s, value %s",
				replicationSitesProperty, e.DeploymentID, e.NodeName, replicationSitesStr)
		}
	}

	// DDI path where to store results
	project, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, projectProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", projectProperty, e.DeploymentID, e.NodeName)
	}
	if project == nil || project.RawString() == "" {
		return nil, 0, errors.Errorf("Mandatory property %s is not set for deployment %s node %s", projectProperty, e.DeploymentID, e.NodeName)
	}

	val, err = deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, metadataProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get metadata property for deployment %s node %s", e.DeploymentID, e.NodeName)
	}

	// Dataset metadata
	var metadata ddi.Metadata
	var metadataStr string
	if val != nil && val.RawString() != "" {
		metadataStr = val.RawString()
		err = json.Unmarshal([]byte(metadataStr), &metadata)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "Failed to parse metadata property for deployment %s node %s, value %s",
				e.DeploymentID, e.NodeName, metadataStr)
		}
	}

	// Get the json value of the operation
	operationStr, err := json.Marshal(e.Operation)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to marshal operation %+v for deployment %s node %s",
			e.Operation, e.DeploymentID, e.NodeName)
	}

	data := make(map[string]string)
	data[actionDataTaskID] = e.TaskID
	data[actionDataNodeName] = e.NodeName
	data[actionDataFilesPatterns] = filesPatternsStr
	data[actionDataElapsedTime] = elapsedTimeStr
	data[actionDataTaskName] = taskName
	data[actionDataOperation] = string(operationStr)
	data[actionDataGroupFilesPattern] = groupFilesPattern
	data[actionDataReplicationSites] = replicationSitesStr
	data[actionDataDDIProjectName] = project.RawString()
	data[actionDataMetadata] = metadataStr
	data[actionDataEncrypt] = encrypt
	data[actionDataCompress] = compress

	return &prov.Action{ActionType: StoreRunningHPCJobFilesGroupByDatasetAction, Data: data}, e.MonitoringTimeInterval, nil
}

// Execute executes a synchronous operation
func (e *StoreRunningHPCJobFilesGroupByDataset) Execute(ctx context.Context) error {

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
			"Submitting job %s", e.NodeName)

		// Initializing the stored files attribute that will be updated by the monitoring task
		storedFiles := make(map[string]StoredFileInfo)
		if err != nil {
			return err
		}
		err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
			storedFilesConsulAttribute, storedFiles)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %+v", e.DeploymentID, e.NodeName, storedFilesConsulAttribute, storedFiles)
		}
		// Initializing the to be stored files attribute that will be updated by the monitoring task
		toBeStoredFiles := make(map[string]ToBeStoredFileInfo)
		err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
			toBeStoredFilesConsulAttribute, toBeStoredFiles)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %+v", e.DeploymentID, e.NodeName, toBeStoredFilesConsulAttribute, toBeStoredFiles)
		}

		// Initializing the map of datasets that will be created by group of files
		datasetReplication := make(map[string]DatasetReplicationInfo)
		err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
			datasetReplicationConsulAttribute, datasetReplication)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %+v", e.DeploymentID, e.NodeName, datasetReplicationConsulAttribute, datasetReplication)
		}

	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		// Nothing to do here
	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}
