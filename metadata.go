package cockroachdb

import (
	"fmt"

	"github.com/project-flogo/core/data"
	"github.com/project-flogo/core/data/coerce"
	"github.com/project-flogo/core/data/mapper"
	"github.com/project-flogo/core/data/property"
	"github.com/project-flogo/core/data/resolve"
)

var resolver = resolve.NewCompositeResolver(map[string]resolve.Resolver{
	".":        &resolve.ScopeResolver{},
	"env":      &resolve.EnvResolver{},
	"property": &property.Resolver{},
	"loop":     &resolve.LoopResolver{},
})

type (
	// Settings struct of Actvity
	Settings struct {
		Database    string                 `md:"database"`
		DataMapping map[string]interface{} `md:"dataMapping"`
		Host        string                 `md:"host"`
		Options     map[string]interface{} `md:"options"`
		Password    string                 `md:"password"`
		User        string                 `md:"user"`
	}

	// Input struct of Activity
	Input struct {
		Data interface{} `md:"data,required"`
	}

	// Output struct of Activity
	Output struct {
		Status string      `md:"status"`
		Result interface{} `md:"result"`
	}
)

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

	s.Options, err = coerce.ToObject(values["options"])
	if err != nil {
		return err
	}

	if s.Options != nil {

		fmt.Println(fmt.Sprintf("s.Options: %v", s.Options))

		var options map[string]interface{}

		mapperFactory := mapper.NewFactory(resolver)
		var optionsMapper mapper.Mapper
		optionsMapper, err = mapperFactory.NewMapper(options)
		if err != nil {
			return err
		}

		var optionsValue map[string]interface{}
		optionsValue, err = optionsMapper.Apply(&data.SimpleScope{})
		if err != nil {
			return err
		}

		s.Options = optionsValue
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
		"database":    s.Database,
		"dataMapping": s.DataMapping,
		"host":        s.Host,
		"options":     s.Options,
		"password":    s.Password,
		"user":        s.User,
	}

}

// FromMap method of Input
func (i *Input) FromMap(values map[string]interface{}) error {
	var err error

	i.Data, err = coerce.ToObject(values["data"])
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
