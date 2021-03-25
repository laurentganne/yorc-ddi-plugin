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

package standard

import (
	"context"
	"strings"
	"time"

	"github.com/laurentganne/yorc-ddi-plugin/common"
	"github.com/laurentganne/yorc-ddi-plugin/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

// DDIAccessExecution holds DDI Access Execution properties
type DDIAccessExecution struct {
	*common.DDIExecution
}

// ExecuteAsync is not supported here
func (e *DDIAccessExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	return nil, 0, errors.Errorf("Unsupported asynchronous operation %s", e.Operation.Name)
}

// Execute executes a synchronous operation
func (e *DDIAccessExecution) Execute(ctx context.Context) error {

	var err error
	var ddiClient ddi.Client
	switch strings.ToLower(e.Operation.Name) {
	case "install", "standard.create", "standard.start":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating %q", e.NodeName)
		ddiClient, err = e.GetDDIClientFromAssociatedComputeLocation(ctx)
		if err != nil {
			return err
		}
		err = e.SetDDIAccessCapabilityAttributes(ctx, ddiClient)
	case "uninstall", "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting %q", e.NodeName)
		// Nothing to do here
	case "standard.stop":
		// Nothing to do
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Executing operation %s on node %q", e.Operation.Name, e.NodeName)
	case tosca.RunnableSubmitOperationName, tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %s", e.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %s", e.Operation.Name)
	}

	return err
}
