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
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tosca"
)

// SSHFSMountStagingAreaDataset holds SSHFS mount properties
type SSHFSMountStagingAreaDataset struct {
	*common.DDIExecution
}

// ExecuteAsync is not supported here
func (s *SSHFSMountStagingAreaDataset) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	return nil, 0, errors.Errorf("Unsupported asynchronous operation %s", s.Operation.Name)
}

// Execute executes a synchronous operation
func (s *SSHFSMountStagingAreaDataset) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(s.Operation.Name) {
	case "custom.refresh_token":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, s.DeploymentID).Registerf(
			"Refreshing token for %s", s.NodeName)
		var locationProps config.DynamicMap
		_, locationProps, err = s.GetDDIClientFromHostingComputeLocation(ctx)
		if err != nil {
			return err
		}
		var accessToken string
		accessToken, _, err = common.RefreshToken(ctx, locationProps, s.DeploymentID)
		if err != nil {
			return err
		}
		// The start operation expects to find the access token in the component attributes
		err = deployments.SetAttributeForAllInstances(ctx, s.DeploymentID, s.NodeName,
			"access_token", accessToken)

	case "standard.create", "standard.start", "standard.stop":
		err = errors.Errorf("Unsupported operation %s in plugin as it is implemented in an Ansible playbook", s.Operation.Name)
	case "install", "uninstall", "standard.delete":
		// Nothing to do here
	case tosca.RunnableSubmitOperationName, tosca.RunnableCancelOperationName:
		err = errors.Errorf("Unsupported operation %s", s.Operation.Name)
	default:
		err = errors.Errorf("Unsupported operation %s", s.Operation.Name)
	}

	return err
}
