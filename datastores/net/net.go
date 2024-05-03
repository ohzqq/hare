package net

type Net struct {
	*store.Store
	name string
}

func New(tableName string, data []byte) (*Net, error) {
	n := &Net{
		Store: store.New(),
		name:  tableName,
	}

	err := n.Store.CreateTable(tableName, store.NewMemFile(data))
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Net) CreateTable(tableName string) error {
	return n.Store.CreateTable(tableName, store.NewMemFile([]byte{}))
}
