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

// LocationHPCStagingArea holds properties of a HPC staging area
type LocationHPCStagingArea struct {
	Name string `yaml:"name" json:"name"`
}

// LocationCloudStagingArea holds properties of a HPC staging area
type LocationCloudStagingArea struct {
	Name             string `yaml:"name" json:"name"`
	RemoteFileSystem string `yaml:"remote_file_system" json:"remote_file_system"`
	MountType        string `yaml:"mount_type" json:"mount_type"`
	MountOptions     string `yaml:"mount_options" json:"mount_options"`
	UserID           string `yaml:"user_id" json:"user_id"`
	GroupID          string `yaml:"group_id" json:"group_id"`
}

// DataTransferRequest holds parameters of a data transfer request
type DataTransferRequest struct {
	SourceSystem string `json:"source_system"`
	SourcePath   string `json:"source_path"`
	TargetSystem string `json:"target_system"`
	TargetPath   string `json:"target_path"`
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
	Status string `json:"status"`
}
