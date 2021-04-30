// Copyright 2020 Bull r.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
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
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

const targetRequirement = "target"

// RefreshTargetTokens holds propertied of a component refreshing tokens of a target
type RefreshTargetTokens struct {
	*common.DDIExecution
}

// ExecuteAsync is not supported here
func (r *RefreshTargetTokens) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	return nil, 0, errors.Errorf("Unsupported asynchronous operation %s", r.Operation.Name)
}

// Execute executes a synchronous operation
func (r *RefreshTargetTokens) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(r.Operation.Name) {
	case "standard.start":
		var locationProps config.DynamicMap
		var targetNodeName string
		_, locationProps, targetNodeName, err = r.GetDDIClientFromRequirement(ctx, targetRequirement)
		if err != nil {
			err = errors.Wrapf(err, "Failed to get target associated to %s", r.NodeName)
		} else {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, r.DeploymentID).Registerf(
				"Refreshing tokens for target of %s: %s", r.NodeName, targetNodeName)
			_, _, err = common.RefreshToken(ctx, locationProps, r.DeploymentID, targetNodeName, "")
		}
	case "install", "uninstall", "standard.create", "standard.stop", "standard.delete":
		// Nothing to do here
	case tosca.RunnableSubmitOperationName, tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %s", r.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %s", r.Operation.Name)
	}

	return err
}
