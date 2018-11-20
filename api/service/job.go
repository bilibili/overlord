package service

import (
	"context"
	"overlord/api/model"
)

// GetJob will get job by given jobID string
func (s *Service) GetJob(jobID string) (*model.Job, error) {
	return s.d.GetJob(context.Background(), jobID)
}

// GetJobs will get job by given jobID string
func (s *Service) GetJobs() ([]*model.Job, error) {
	return s.d.GetJobs(context.Background())
}

// ApproveJob will approve job and change the state from StateWaitApprove to StateDone
func (s Service) ApproveJob(jobID string) error {
	return s.d.ApproveJob(context.Background(), jobID)
}
