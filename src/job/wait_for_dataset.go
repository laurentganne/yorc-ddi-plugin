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
	"strings"
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/v1/common"
	"github.com/laurentganne/yorc-ddi-plugin/v1/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

// WaitForDataset holds DDI to HPC data transfer job Execution properties
type WaitForDataset struct {
	*common.DDIExecution
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (e *WaitForDataset) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	if strings.ToLower(e.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", e.Operation.Name)
	}

	val, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, metadataProperty)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "Failed to get metadata property for deployment %s node %s", e.DeploymentID, e.NodeName)
	}
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

	data := make(map[string]string)
	data[actionDataTaskID] = e.TaskID
	data[actionDataNodeName] = e.NodeName
	data[actionDataToken] = e.Token
	data[actionDataMetadata] = metadataStr
	data[actionDataFilesPatterns] = filesPatternsStr

	return &prov.Action{ActionType: WaitForDatasetAction, Data: data}, e.MonitoringTimeInterval, nil
}

// Execute executes a synchronous operation
func (e *WaitForDataset) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting Wait For Dataset request %q", e.NodeName)
		// Nothing to do here
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		// Nothing to do here
	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}
