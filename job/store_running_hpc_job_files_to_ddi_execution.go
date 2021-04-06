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
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

// StoreRunningHPCJobFilesToDDI holds DDI to HPC data transfer job Execution properties
type StoreRunningHPCJobFilesToDDI struct {
	*DDIJobExecution
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (e *StoreRunningHPCJobFilesToDDI) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
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

	// Get the json value of the operation
	operationStr, err := json.Marshal(e.Operation)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to marshal operation %+v for deployment %s node %s",
			e.Operation, e.DeploymentID, e.NodeName)
	}

	data := make(map[string]string)
	data[actionDataTaskID] = e.TaskID
	data[actionDataNodeName] = e.NodeName
	data[actionDataToken] = e.Token
	data[actionDataFilesPatterns] = filesPatternsStr
	data[actionDataElapsedTime] = elapsedTimeStr
	data[actionDataTaskName] = taskName
	data[actionDataOperation] = string(operationStr)

	return &prov.Action{ActionType: StoreRunningHPCJobFilesToDDIAction, Data: data}, e.MonitoringTimeInterval, nil
}

// Execute executes a synchronous operation
func (e *StoreRunningHPCJobFilesToDDI) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		var locationName string
		locationName, err = e.setLocationFromAssociatedHPCJob(ctx)
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Location for %s is %s", e.NodeName, locationName)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Dataset where to store results for %q", e.NodeName)

		ddiClient, err := getDDIClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
		if err != nil {
			return err
		}

		// DDI path where to store results
		project, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, projectProperty)
		if err != nil {
			return errors.Wrapf(err, "Failed to get %s property for deployment %s node %s", projectProperty, e.DeploymentID, e.NodeName)
		}
		if project == nil || project.RawString() == "" {
			return errors.Errorf("Mandatory property %s is not set for deployment %s node %s", projectProperty, e.DeploymentID, e.NodeName)
		}
		ddiPath := getDDIProjectPath(project.RawString())

		val, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, metadataProperty)
		if err != nil {
			return errors.Wrapf(err, "Failed to get metadata property for deployment %s node %s", e.DeploymentID, e.NodeName)
		}
		var metadata ddi.Metadata
		var metadataStr string
		if val != nil && val.RawString() != "" {
			metadataStr = val.RawString()
			err = json.Unmarshal([]byte(metadataStr), &metadata)
			if err != nil {
				return errors.Wrapf(err, "Failed to parse metadata property for deployment %s node %s, value %s",
					e.DeploymentID, e.NodeName, metadataStr)
			}
		}

		internalID, err := ddiClient.CreateEmptyDatasetInProject(e.Token, project.RawString(), metadata)
		if err != nil {
			return errors.Wrapf(err, "Failed to create result dataset for deployment %s node %s", e.DeploymentID, e.NodeName)
		}

		datasetPath := path.Join(ddiPath, internalID)

		err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
			destinationDatasetPathConsulAttribute, datasetPath)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %s", e.DeploymentID, e.NodeName, destinationDatasetPathConsulAttribute, "")
		}

		log.Debugf("created dataset ID %s path %s\n", internalID, datasetPath)

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
		if err != nil {
			return err
		}
		err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
			toBeStoredFilesConsulAttribute, toBeStoredFiles)
		if err != nil {
			return errors.Wrapf(err, "Failed to store %s %s %s value %+v", e.DeploymentID, e.NodeName, toBeStoredFilesConsulAttribute, toBeStoredFiles)
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
