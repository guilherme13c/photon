package storage

import "context"

type MockStorage struct {
	SaveFunc  func(ctx context.Context, doc Document) (string, error)
	CloseFunc func() error
}

func (m *MockStorage) Save(ctx context.Context, doc Document) (string, error) {
	if m.SaveFunc != nil {
		return m.SaveFunc(ctx, doc)
	}
	return "mock-key", nil
}

func (m *MockStorage) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}
