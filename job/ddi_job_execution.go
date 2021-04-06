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

	"github.com/laurentganne/yorc-ddi-plugin/common"
	"github.com/laurentganne/yorc-ddi-plugin/ddi"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/storage"
	storageTypes "github.com/ystia/yorc/v4/storage/types"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	installOperation                      = "install"
	uninstallOperation                    = "uninstall"
	metadataProperty                      = "metadata"
	filesPatternProperty                  = "needed_files_patterns"
	elapsedTimeMinutesProperty            = "last_modification_elapsed_time_minutes"
	projectProperty                       = "project"
	taskNameProperty                      = "task_name"
	requestIDConsulAttribute              = "request_id"
	destinationDatasetPathConsulAttribute = "destination_path"
	storedFilesConsulAttribute            = "stored_files"
	toBeStoredFilesConsulAttribute        = "to_be_stored_files"
	stagingAreaNameConsulAttribute        = "staging_area_name"
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
	jobChangedFilesEnvVar                 = "JOB_CHANGED_FILES"
	jobStartDataEnvVar                    = "JOB_START_DATE"
	jobStateEnvVar                        = "JOB_STATE"
	dataTransferCapability                = "data_transfer"
	datasetFilesProviderCapability        = "dataset_files"
	osCapability                          = "tosca.capabilities.OperatingSystem"
	heappeJobCapability                   = "org.lexis.common.heappe.capabilities.HeappeJob"
	cloudAreaDirProviderCapability        = "org.lexis.common.ddi.capabilities.CloudAreaDirectoryProvider"
	dataTransferCloudCapability           = "org.lexis.common.ddi.capabilities.DataTransferCloud"
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

func (e *DDIJobExecution) getDDIAreaNames(ctx context.Context) ([]string, error) {

	var ddiAreaNames []string
	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return ddiAreaNames, err
	}

	locations, err := locationMgr.GetLocations()
	if err != nil {
		return ddiAreaNames, err
	}

	for _, loc := range locations {
		if loc.Type == common.DDIInfrastructureType {
			locationProps, err := locationMgr.GetLocationProperties(loc.Name, common.DDIInfrastructureType)
			if err != nil {
				return ddiAreaNames, err
			}
			ddiArea := locationProps.GetString(ddi.LocationDDIAreaPropertyName)
			if ddiArea == "" {
				return ddiAreaNames, errors.Errorf("No %s property defined in DDI location configuration", ddi.LocationDDIAreaPropertyName)
			}

			ddiAreaNames = append(ddiAreaNames, ddiArea)
		}
	}

	return ddiAreaNames, err
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

func getDDIClientAlive(ctx context.Context, cfg config.Configuration, deploymentID, nodeName string) (ddi.Client, string, error) {

	// First attempt to get the location defined in node metadata if any
	var ddiClient ddi.Client
	var locationName string
	found, locationName, err := deployments.GetNodeMetadata(ctx, deploymentID, nodeName, tosca.MetadataLocationNameKey)
	if err != nil {
		return ddiClient, locationName, err
	}

	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return ddiClient, locationName, err
	}

	if found {
		// Check if the corresponding DDI client is alive
		locationProps, err := locationMgr.GetLocationProperties(locationName, common.DDIInfrastructureType)
		if err != nil {
			return ddiClient, locationName, err
		}
		ddiClient, err = ddi.GetClient(locationProps)
		if err != nil {
			return ddiClient, locationName, err
		}
		if ddiClient.IsAlive() {
			return ddiClient, locationName, err
		} else {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).Registerf(
				"DDI location %s specified in node %s metadata is unreachable", locationName, nodeName)

		}
	}

	// Get the first DDI client alive
	locations, err := locationMgr.GetLocations()
	if err != nil {
		return ddiClient, locationName, err
	}
	for _, loc := range locations {
		if loc.Type == common.DDIInfrastructureType {
			locationProps, err := locationMgr.GetLocationProperties(loc.Name, common.DDIInfrastructureType)
			if err != nil {
				return ddiClient, locationName, err
			}
			ddiClient, err = ddi.GetClient(locationProps)
			if err != nil {
				return ddiClient, locationName, err
			}
			if ddiClient.IsAlive() {
				locationName = loc.Name
				return ddiClient, locationName, err
			} else {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).Registerf(
					"DDI location %s is unreachable", loc.Name)
			}
		}
	}

	return ddiClient, locationName, errors.Errorf("Found no DDI location currently reachable")
}

func setNodeMetadataLocation(ctx context.Context, cfg config.Configuration, deploymentID, nodeName, locationName string) error {
	nodeTemplate, err := getStoredNodeTemplate(ctx, deploymentID, nodeName)
	if err != nil {
		return err
	}
	// Add the new location in this node template metadata
	if nodeTemplate.Metadata == nil {
		nodeTemplate.Metadata = make(map[string]string)
	}
	nodeTemplate.Metadata[tosca.MetadataLocationNameKey] = locationName
	// Location is now changed for this node template, storing it
	err = storeNodeTemplate(ctx, deploymentID, nodeName, nodeTemplate)
	return err
}

// getStoredNodeTemplate returns the description of a node stored by Yorc
func getStoredNodeTemplate(ctx context.Context, deploymentID, nodeName string) (*tosca.NodeTemplate, error) {
	node := new(tosca.NodeTemplate)
	nodePath := path.Join(consulutil.DeploymentKVPrefix, deploymentID, "topology", "nodes", nodeName)
	found, err := storage.GetStore(storageTypes.StoreTypeDeployment).Get(nodePath, node)
	if !found {
		err = errors.Errorf("No such node %s in deployment %s", nodeName, deploymentID)
	}
	return node, err
}

// storeNodeTemplate stores a node template in Yorc
func storeNodeTemplate(ctx context.Context, deploymentID, nodeName string, nodeTemplate *tosca.NodeTemplate) error {
	nodePrefix := path.Join(consulutil.DeploymentKVPrefix, deploymentID, "topology", "nodes", nodeName)
	return storage.GetStore(storageTypes.StoreTypeDeployment).Set(ctx, nodePrefix, nodeTemplate)
}

// GetDDILocationNameFromComputeLocationName gets the DDI location name
// for the location on which the associated compute instance is running if any
func (e *DDIJobExecution) GetDDILocationNameFromInfrastructureLocation(ctx context.Context,
	infraLocation string) (string, error) {

	var locationName string
	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return locationName, err
	}
	// Convention: the first section of location identify the datacenter
	dcID := strings.ToLower(strings.SplitN(infraLocation, "_", 2)[0])
	locations, err := locationMgr.GetLocations()
	if err != nil {
		return locationName, err
	}

	for _, loc := range locations {
		if loc.Type == common.DDIInfrastructureType && strings.HasPrefix(strings.ToLower(loc.Name), dcID) {
			return loc.Name, err
		}
	}

	return locationName, err
}

// setLocationFromAssociatedCloudInstance sets the location of this component
// according to an associated compute instance location
func (e *DDIJobExecution) setLocationFromAssociatedCloudInstance(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedTarget(ctx, osCapability)
}

// setLocationFromAssociatedHPCJob sets the location of this component
// according to an associated HPC location
func (e *DDIJobExecution) setLocationFromAssociatedHPCJob(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedTarget(ctx, heappeJobCapability)
}

// setLocationFromAssociatedTarget sets the location of this component
// according to an associated target
func (e *DDIJobExecution) setLocationFromAssociatedTarget(ctx context.Context, targetCapability string) (string, error) {
	var locationName string
	nodeTemplate, err := e.getStoredNodeTemplate(ctx, e.NodeName)
	if err != nil {
		return locationName, err
	}

	// Get the associated target node name if any
	var targetNodeName string
	for _, nodeReq := range nodeTemplate.Requirements {
		for _, reqAssignment := range nodeReq {
			if reqAssignment.Capability == targetCapability {
				targetNodeName = reqAssignment.Node
				break
			}
		}
	}
	if targetNodeName == "" {
		return locationName, err
	}

	// Get the target location
	targetNodeTemplate, err := e.getStoredNodeTemplate(ctx, targetNodeName)
	if err != nil {
		return locationName, err
	}
	var targetLocationName string
	if targetNodeTemplate.Metadata != nil {
		targetLocationName = targetNodeTemplate.Metadata[tosca.MetadataLocationNameKey]
	}
	if targetLocationName == "" {
		return locationName, err
	}

	// Get the corresponding DDI location
	locationName, err = e.GetDDILocationNameFromInfrastructureLocation(ctx, targetLocationName)
	if err != nil || locationName == "" {
		return locationName, err
	}

	// Store the location name in this node template metadata
	if nodeTemplate.Metadata == nil {
		nodeTemplate.Metadata = make(map[string]string)
	}
	nodeTemplate.Metadata[tosca.MetadataLocationNameKey] = locationName
	// Location is now changed for this node template, storing it
	err = e.storeNodeTemplate(ctx, e.NodeName, nodeTemplate)
	return locationName, err
}

// setLocationFromAssociatedCloudAreaDirectoryProvider sets the location of this component
// according to an associated component providing a directoy in a cloud area
func (e *DDIJobExecution) setLocationFromAssociatedCloudAreaDirectoryProvider(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedCloudProvider(ctx, cloudAreaDirProviderCapability)
}

// setLocationFromAssociatedCloudAreaDirectoryProvider sets the location of this component
// according to an associated component providing a directoy in a cloud area
func (e *DDIJobExecution) setLocationFromAssociatedCloudDataTransfer(ctx context.Context) (string, error) {
	return e.setLocationFromAssociatedCloudProvider(ctx, dataTransferCloudCapability)
}

// setLocationFromAssociatedCloudAreaDirectoryProvider sets the location of this component
// according to an associated component providing a directoy in a cloud area
func (e *DDIJobExecution) setLocationFromAssociatedCloudProvider(ctx context.Context, capabilityName string) (string, error) {

	var locationName string
	nodeTemplate, err := e.getStoredNodeTemplate(ctx, e.NodeName)
	if err != nil {
		return locationName, err
	}

	// Get the associated target node name if any
	var targetNodeName string
	for _, nodeReq := range nodeTemplate.Requirements {
		for _, reqAssignment := range nodeReq {
			if reqAssignment.Capability == capabilityName {
				targetNodeName = reqAssignment.Node
				break
			}
		}
	}
	if targetNodeName == "" {
		return locationName, err
	}

	val, err := deployments.GetInstanceAttributeValue(ctx, e.DeploymentID, targetNodeName, "0", stagingAreaNameConsulAttribute)
	if err != nil {
		return locationName, errors.Wrapf(err, "Failed to get staging area name for deployment %s node %s", e.DeploymentID, targetNodeName)
	} else if val == nil {
		return locationName, errors.Errorf("Found no staging area name for deployment %s node %s", e.DeploymentID, targetNodeName)
	}

	stagingAreaName := val.RawString()
	locationMgr, err := locations.GetManager(e.Cfg)
	if err != nil {
		return locationName, err
	}

	locations, err := locationMgr.GetLocations()
	if err != nil {
		return locationName, err
	}
	for _, loc := range locations {
		if loc.Type == common.DDIInfrastructureType && loc.Properties.GetString(ddi.LocationCloudStagingAreaNamePropertyName) == stagingAreaName {
			locationName = loc.Name
			break
		}
	}

	if locationName == "" {
		return locationName, err
	}

	// Store the location name in this node template metadata
	if nodeTemplate.Metadata == nil {
		nodeTemplate.Metadata = make(map[string]string)
	}
	nodeTemplate.Metadata[tosca.MetadataLocationNameKey] = locationName
	// Location is now changed for this node template, storing it
	err = e.storeNodeTemplate(ctx, e.NodeName, nodeTemplate)
	return locationName, err

}

// getStoredNodeTemplate returns the description of a node stored by Yorc
func (e *DDIJobExecution) getStoredNodeTemplate(ctx context.Context, nodeName string) (*tosca.NodeTemplate, error) {
	node := new(tosca.NodeTemplate)
	nodePath := path.Join(consulutil.DeploymentKVPrefix, e.DeploymentID, "topology", "nodes", nodeName)
	found, err := storage.GetStore(storageTypes.StoreTypeDeployment).Get(nodePath, node)
	if !found {
		err = errors.Errorf("No such node %s in deployment %s", nodeName, e.DeploymentID)
	}
	return node, err
}

// storeNodeTemplate stores a node template in Yorc
func (e *DDIJobExecution) storeNodeTemplate(ctx context.Context, nodeName string, nodeTemplate *tosca.NodeTemplate) error {
	nodePrefix := path.Join(consulutil.DeploymentKVPrefix, e.DeploymentID, "topology", "nodes", nodeName)
	return storage.GetStore(storageTypes.StoreTypeDeployment).Set(ctx, nodePrefix, nodeTemplate)
}
