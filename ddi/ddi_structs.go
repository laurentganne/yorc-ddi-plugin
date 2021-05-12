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

package ddi

// LocationCloudStagingArea holds properties of a HPC staging area
type LocationCloudStagingArea struct {
	Name             string `yaml:"name" json:"name"`
	RemoteFileSystem string `yaml:"remote_file_system" json:"remote_file_system"`
	MountType        string `yaml:"mount_type" json:"mount_type"`
	MountOptions     string `yaml:"mount_options" json:"mount_options"`
	UserID           string `yaml:"user_id" json:"user_id"`
	GroupID          string `yaml:"group_id" json:"group_id"`
}

// Metadata holds metadata to define for a dataset
type Metadata struct {
	Creator           []string `json:"creator,omitempty"`
	Contributor       []string `json:"contributor,omitempty"`
	Publisher         []string `json:"publisher,omitempty"`
	Owner             []string `json:"owner,omitempty"`
	Identifier        string   `json:"identifier,omitempty"`
	PublicationYear   string   `json:"publicationYear,omitempty"`
	ResourceType      string   `json:"resourceType,omitempty"`
	Title             string   `json:"title,omitempty"`
	RelatedIdentifier []string `json:"relatedIdentifier,omitempty"`
}

// DataTransferRequest holds parameters of a data transfer request
type DataTransferRequest struct {
	Metadata     Metadata `json:"metadata,omitempty"`
	SourceSystem string   `json:"source_system"`
	SourcePath   string   `json:"source_path"`
	TargetSystem string   `json:"target_system"`
	TargetPath   string   `json:"target_path,omitempty"`
	Encryption   string   `json:"encryption"`
	Compression  string   `json:"compression"`
}

// DataTransferRequestHPCExtension holds additional parameters for data transfers on HPC
type DataTransferRequestHPCExtension struct {
	HEAppEURL string `json:"heappe_url"`
	JobID     int64  `json:"job_id"`
	TaskID    int64  `json:"task_id,omitempty"`
}

// HPCDataTransferRequest holds parameters of a data transfer request
type HPCDataTransferRequest struct {
	DataTransferRequest
	DataTransferRequestHPCExtension
}

// DeleteDataRequest holds parameters of data to delete
type DeleteDataRequest struct {
	TargetSystem string `json:"target_system"`
	TargetPath   string `json:"target_path"`
}

// SubmittedRequestInfo holds the result of a request submission
type SubmittedRequestInfo struct {
	RequestID string `json:"request_id"`
}

// RequestStatus holds the status of a submitted request
type RequestStatus struct {
	Status     string `json:"status"`
	TargetPath string `json:"target_path,omitempty"`
	PID        string `json:"PID,omitempty"`
}

// DatasetLocation holds location properties of a dataset
type DatasetLocation struct {
	InternalID string `json:"internalID"`
	Access     string `json:"access"`
	Project    string `json:"project"`
}

// DatasetSearchResult holds properties of an element of a dataset search result
type DatasetSearchResult struct {
	Location DatasetLocation `json:"location"`
	Metadata Metadata        `json:"metadata"`
}

// DatasetCreateRequest holds properties of a request to create a dataset
type DatasetCreateRequest struct {
	PushMethod string   `json:"push_method"`
	Access     string   `json:"access"`
	Project    string   `json:"project,omitempty"`
	Metadata   Metadata `json:"metadata,omitempty"`
}

// DatasetCreateResponse holds a dataset creation response
type DatasetCreateResponse struct {
	Status     string `json:"status"`
	InternalID string `json:"internalID"`
}

// DatasetListingRequest holds properties of a request to list the content of a dataset
type DatasetListingRequest struct {
	InternalID string `json:"internalID"`
	Access     string `json:"access"`
	Project    string `json:"project"`
	Recursive  bool   `json:"recursive"`
}

// DatasetListing holds the listing of a dataset content
type DatasetListing struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Contents []*DatasetListing `json:"contents,omitempty"`
}

// ReplicationStatusRequest holds parameters of a replication status request
type ReplicationStatusRequest struct {
	TargetSystem string `json:"target_system"`
	TargetPath   string `json:"target_path"`
}

// ReplicationStatusResponse holds the status of a replication status request
type ReplicationStatusResponse struct {
	Status string `json:"status,omitempty"`
}

// DatasetInfoRequest holds parameters of a dataset info request
type DatasetInfoRequest struct {
	TargetSystem string `json:"target_system"`
	TargetPath   string `json:"target_path"`
}

// DatasetInfoResponse holds parameters of a dataset info request response
type DatasetInfo struct {
	Result             string `json:"result"`
	Size               string `json:"size,omitempty"`
	NumberOfFiles      string `json:"totalfiles,omitempty"`
	NumberOfSmallFiles string `json:"smallfiles,omitempty"`
}
