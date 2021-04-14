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

package main

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/laurentganne/yorc-ddi-plugin/common"
	"github.com/laurentganne/yorc-ddi-plugin/job"
	"github.com/laurentganne/yorc-ddi-plugin/standard"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/prov"
)

const (
	ddiInfrastructureType                   = "ddi"
	locationJobMonitoringTimeInterval       = "job_monitoring_time_interval"
	locationDefaultMonitoringTimeInterval   = 5 * time.Second
	ddiAccessComponentType                  = "org.lexis.common.ddi.nodes.DDIAccess"
	computeInstanceDatasetInfoComponentType = "org.lexis.common.ddi.nodes.GetComputeInstanceDatasetInfo"
	hpcJobTaskDatasetInfoComponentType      = "org.lexis.common.ddi.nodes.GetHPCJobTaskDatasetInfo"
	enableCloudStagingAreaJobType           = "org.lexis.common.ddi.nodes.EnableCloudStagingAreaAccessJob"
	disableCloudStagingAreaJobType          = "org.lexis.common.ddi.nodes.DisableCloudStagingAreaAccessJob"
	ddiToCloudJobType                       = "org.lexis.common.ddi.nodes.DDIToCloudJob"
	ddiToHPCTaskJobType                     = "org.lexis.common.ddi.nodes.DDIToHPCTaskJob"
	hpcToDDIJobType                         = "org.lexis.common.ddi.nodes.pub.HPCToDDIJob"
	ddiRuntimeToCloudJobType                = "org.lexis.common.ddi.nodes.DDIRuntimeToCloudJob"
	ddiRuntimeToHPCTaskJobType              = "org.lexis.common.ddi.nodes.DDIRuntimeToHPCTaskJob"
	cloudToDDIJobType                       = "org.lexis.common.ddi.nodes.CloudToDDIJob"
	waitForDDIDatasetJobType                = "org.lexis.common.ddi.nodes.pub.WaitForDDIDatasetJob"
	storeRunningHPCJobType                  = "org.lexis.common.ddi.nodes.pub.StoreRunningHPCJobFilesToDDIJob"
	deleteCloudDataJobType                  = "org.lexis.common.ddi.nodes.DeleteCloudDataJob"
	getDDIDatasetInfoJobType                = "org.lexis.common.ddi.nodes.GetDDIDatasetInfoJob"
)

// Execution is the interface holding functions to execute an operation
type Execution interface {
	ResolveExecution(ctx context.Context) error
	ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error)
	Execute(ctx context.Context) error
}

func newExecution(ctx context.Context, cfg config.Configuration, taskID, deploymentID, nodeName string,
	operation prov.Operation) (Execution, error) {

	consulClient, err := cfg.GetConsulClient()
	if err != nil {
		return nil, err
	}
	kv := consulClient.KV()

	var exec Execution

	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return nil, err
	}
	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx,
		deploymentID, nodeName, ddiInfrastructureType)
	if err != nil {
		return nil, err
	}

	monitoringTimeInterval := locationProps.GetDuration(locationJobMonitoringTimeInterval)
	if monitoringTimeInterval <= 0 {
		// Default value
		monitoringTimeInterval = locationDefaultMonitoringTimeInterval
	}

	isDDIAccessComponent, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, ddiAccessComponentType)
	if err != nil {
		return exec, err
	}

	if isDDIAccessComponent {
		exec = &standard.DDIAccessExecution{
			DDIExecution: &common.DDIExecution{
				KV:           kv,
				Cfg:          cfg,
				DeploymentID: deploymentID,
				TaskID:       taskID,
				NodeName:     nodeName,
				Operation:    operation,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	isComputeDatasetInfoComponent, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, computeInstanceDatasetInfoComponentType)
	if err != nil {
		return exec, err
	}

	if isComputeDatasetInfoComponent {
		exec = &standard.ComputeDatasetInfoExecution{
			DDIExecution: &common.DDIExecution{
				KV:           kv,
				Cfg:          cfg,
				DeploymentID: deploymentID,
				TaskID:       taskID,
				NodeName:     nodeName,
				Operation:    operation,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	isHPCJobTaskDatasetInfo, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, hpcJobTaskDatasetInfoComponentType)
	if err != nil {
		return exec, err
	}

	if isHPCJobTaskDatasetInfo {
		exec = &standard.HPCDatasetInfoExecution{
			DDIExecution: &common.DDIExecution{
				KV:           kv,
				Cfg:          cfg,
				DeploymentID: deploymentID,
				TaskID:       taskID,
				NodeName:     nodeName,
				Operation:    operation,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	// Other executions require a token
	token, err := deployments.GetStringNodePropertyValue(ctx, deploymentID, nodeName, "accessToken")
	if err != nil {
		return exec, err
	}
	if token == "" {
		return exec, errors.Errorf("No value provided for deployement %s node %s proerty token", deploymentID, nodeName)
	}

	isDDIDatasetInfoJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, getDDIDatasetInfoJobType)
	if err != nil {
		return exec, err
	}
	if isDDIDatasetInfoJob {
		exec = &job.DDIDatasetInfoExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.GetDDIDatasetInfoAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isDeleteCloudDataJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, deleteCloudDataJobType)
	if err != nil {
		return exec, err
	}
	if isDeleteCloudDataJob {
		exec = &job.DeleteCloudDataExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.CloudDataDeleteAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	isDDIToCloudJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, ddiToCloudJobType)
	if err != nil {
		return exec, err
	}
	if isDDIToCloudJob {
		exec = &job.DDIToCloudExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isDDIRuntimeToCloudJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, ddiRuntimeToCloudJobType)
	if err != nil {
		return exec, err
	}
	if isDDIRuntimeToCloudJob {
		exec = &job.DDIRuntimeToCloudExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isCloudToDDIJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, cloudToDDIJobType)
	if err != nil {
		return exec, err
	}
	if isCloudToDDIJob {
		exec = &job.CloudToDDIJobExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isDDIToHPCTaskJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, ddiToHPCTaskJobType)
	if err != nil {
		return exec, err
	}
	if isDDIToHPCTaskJob {
		exec = &job.DDIToHPCExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isHPCToDDIJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, hpcToDDIJobType)
	if err != nil {
		return exec, err
	}
	if isHPCToDDIJob {
		exec = &job.HPCToDDIExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isDDIRuntimeToHPCTaskJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, ddiRuntimeToHPCTaskJobType)
	if err != nil {
		return exec, err
	}
	if isDDIRuntimeToHPCTaskJob {
		exec = &job.DDIRuntimeToHPCExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DataTransferAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isWaitForDDIDatasetJobType, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, waitForDDIDatasetJobType)
	if err != nil {
		return exec, err
	}
	if isWaitForDDIDatasetJobType {
		exec = &job.WaitForDataset{
			DDIExecution: &common.DDIExecution{
				KV:           kv,
				Cfg:          cfg,
				DeploymentID: deploymentID,
				TaskID:       taskID,
				NodeName:     nodeName,
				Token:        token,
				Operation:    operation,
			},
			MonitoringTimeInterval: monitoringTimeInterval,
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isStoreRunningHPCJobType, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, storeRunningHPCJobType)
	if err != nil {
		return exec, err
	}
	if isStoreRunningHPCJobType {
		exec = &job.StoreRunningHPCJobFilesToDDI{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.StoreRunningHPCJobFilesToDDIAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isEnableCloudStagingAreaJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, enableCloudStagingAreaJobType)
	if err != nil {
		return exec, err
	}

	if isEnableCloudStagingAreaJob {
		exec = &job.EnableCloudAccessJobExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.EnableCloudAccessAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	isDisableCloudStagingAreaJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, disableCloudStagingAreaJobType)
	if err != nil {
		return exec, err
	}

	if isDisableCloudStagingAreaJob {
		exec = &job.DisableCloudAccessJobExecution{
			DDIJobExecution: &job.DDIJobExecution{
				DDIExecution: &common.DDIExecution{
					KV:           kv,
					Cfg:          cfg,
					DeploymentID: deploymentID,
					TaskID:       taskID,
					NodeName:     nodeName,
					Token:        token,
					Operation:    operation,
				},
				ActionType:             job.DisableCloudAccessAction,
				MonitoringTimeInterval: monitoringTimeInterval,
			},
		}
		return exec, exec.ResolveExecution(ctx)
	}

	return exec, errors.Errorf("operation %q supported only for nodes derived from %q",
		operation, ddiToCloudJobType)
}
