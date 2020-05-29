package cockroachdb

import (
	"github.com/project-flogo/core/data/coerce"
)

// Settings struct of Actvity
type Settings struct {
	Database string `md:"database"`
	DataMapping map[string]interface{} `md:"dataMapping"`
	Host string `md:"host"`
	Options map[string]string `md:"options"`
	Password string `md:"password"`
	User string `md:"user"`
}

// FromMap method of Settings
func (s *Settings) FromMap(values map[string]interface{}) error {

	var (
		err error
	)

	s.Database, err = coerce.ToString(values["database"])
	if err != nil {
		return err
	}

	s.DataMapping, err = coerce.ToObject(values["dataMapping"])
	if err != nil {
		return err
	}

	s.Host, err = coerce.ToString(values["host"])
	if err != nil {
		return err
	}

	if values["options"] != nil {
		var options map[string]interface{}
		options, err = coerce.ToObject(values["options"])
		if err != nil {
			return err
		}
		s.Options = make(map[string]string)
		for k, v := range options {
			s.Options[k] = v.(string)
		}
	}

	s.Password, err = coerce.ToString(values["password"])
	if err != nil {
		return err
	}

	s.User, err = coerce.ToString(values["user"])
	if err != nil {
		return err
	}

	return nil

}

// ToMap method of Settings
func (s *Settings) ToMap() map[string]interface{} {

	return map[string]interface{}{
		"database": s.Database,
		"dataMapping": s.DataMapping,
		"host": s.Host,
		"options": s.Options,
		"password": s.Password,
		"user": s.User,
	}

}

// Input struct of Activity
type Input struct {
	Data interface{} `md:"data,required"`
}

// FromMap method of Input
func (i *Input) FromMap(values map[string]interface{}) error {
	var err error

	i.Data, err = coerce.ToAny(values["data"])
	if err != nil {
		return err
	}

	return nil
}

// ToMap method of Input
func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"data": i.Data,
	}
}

// Output struct of Activity
type Output struct {
	Status string `md:"status"`
	Result interface{} `md:"result"`
}

// FromMap method of Output
func (o *Output) FromMap(values map[string]interface{}) error {
	var err error

	o.Status, err = coerce.ToString(values["status"])
	if err != nil {
		return err
	}

	o.Result, err = coerce.ToAny(values["result"])
	if err != nil {
		return err
	}

	return nil
}

// ToMap method of Output
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"status": o.Status,
		"result": o.Result,
	}
}
