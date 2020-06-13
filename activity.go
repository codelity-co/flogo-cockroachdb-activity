package cockroachdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/project-flogo/core/activity"

	"github.com/google/uuid"
	jsonpath "github.com/oliveagle/jsonpath"
	funk "github.com/thoas/go-funk"
	"upper.io/db.v3/lib/sqlbuilder"
	"upper.io/db.v3/postgresql"
)

var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

func init() {
	_ = activity.Register(&Activity{}, New)
}

// New function is used as Activity Factory during registration
func New(ctx activity.InitContext) (activity.Activity, error) {

	var (
		dbSession sqlbuilder.Database
	)

	// Map settings
	s := &Settings{}
	err := s.FromMap(ctx.Settings())
	if err != nil {
		return nil, err
	}
	ctx.Logger().Debugf("Settings: %v", s)

	// Open db connection
	dbSession, err = postgresql.Open(postgresql.ConnectionURL{
		Database: s.Database,
		Host:     s.Host,
		Options:  mapOptionsToString(s.Options),
		Password: s.Password,
		User:     s.User,
	})
	if err != nil {
		return nil, err
	}

	dbSession.SetLogging(true)

	// Create activity
	act := &Activity{
		activitySettings: s,
		dbSession:        dbSession,
	}
	ctx.Logger().Debug("Finished New method of activity")

	return act, nil
}

// Activity struct
type Activity struct {
	activitySettings *Settings
	dbSession        sqlbuilder.Database
}

// Metadata method of Activity struct
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

// Eval method of Activity struct
func (a *Activity) Eval(ctx activity.Context) (bool, error) {

	var err error
	logger := ctx.Logger()

	// Read input data
	input := &Input{}
	err = ctx.GetInputObject(input)
	logger.Debugf("Input: %v", input)
	if err != nil {
		return true, err
	}
	data := input.Data.(map[string]interface{})

	// Prepare SQL Statement
	var res map[string]interface{}
	var txn sqlbuilder.Tx
	txn, err = a.dbSession.NewTx(context.Background())
	if err != nil {
		return true, err
	}

	output := &Output{}

	for k, v := range a.activitySettings.DataMapping {
		collection := k
		method := strings.ToUpper(v.(map[string]interface{})["method"].(string))
		mapping := v.(map[string]interface{})["data"].(map[string]interface{})
		switch method {
		case "INSERT":
			res, err = a.insertCollection(ctx, txn, collection, mapping, data)
			if err != nil {
				_ = txn.Rollback()
				output.Status = "ERROR"
				output.Result = err
				ctx.Logger().Debugf("Output: %v", output)
				_ = ctx.SetOutputObject(output)
				return true, err
			}
		case "UPDATE":
			res, err = a.updateCollection(ctx, txn, collection, mapping, data)
			if err != nil {
				_ = txn.Rollback()
				output.Status = "ERROR"
				output.Result = err
				ctx.Logger().Debugf("Output: %v", output)
				_ = ctx.SetOutputObject(output)
				return true, err
			}

		case "DELETE":
			res, err = a.deleteCollection(ctx, txn, collection, mapping, data)
			if err != nil {
				_ = txn.Rollback()
				output.Status = "ERROR"
				output.Result = err
				ctx.Logger().Debugf("Output: %v", output)
				_ = ctx.SetOutputObject(output)
				return true, err
			}
		case "UPSERT":
			res, err = a.upsertCollection(ctx, txn, collection, mapping, data)
			if err != nil {
				_ = txn.Rollback()
				output.Status = "ERROR"
				output.Result = err
				ctx.Logger().Debugf("Output: %v", output)
				_ = ctx.SetOutputObject(output)
				return true, err
			}
		default:
			_ = txn.Rollback()
			output.Status = "ERROR"
			output.Result = err
			_ = ctx.SetOutputObject(output)
			return true, fmt.Errorf("DB Method is not valid: %s", method)
		}
	}
	err = txn.Commit()
	if err != nil {
		_ = txn.Rollback()
		output.Status = "ERROR"
		output.Result = err
		_ = ctx.SetOutputObject(output)
		return true, err
	}

	// Output result
	output.Status = "SUCCESS"
	output.Result = res
	ctx.Logger().Debugf("Output: %v", output)
	err = ctx.SetOutputObject(output)
	if err != nil {
		return true, err
	}

	return true, nil
}

// Cleanup method of Activity struct
func (a *Activity) Cleanup(ctx activity.Context) error {
	a.dbSession.Close()
	return nil
}

func (a *Activity) insertCollection(ctx activity.Context, txn sqlbuilder.Tx, collection string, mapping map[string]interface{}, data map[string]interface{}) (map[string]interface{}, error) {
	columns, values := a.mapDbFields(ctx, mapping, data)
	if len(values) == 0 {
		return nil, fmt.Errorf("Cannot map any value")
	}
	pos := funk.IndexOf(columns, "id")
	res, err := txn.InsertInto(collection).Columns(columns...).Values(values...).Exec()
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	return map[string]interface{}{
		"lastInsertedID": values[pos],
		"rowsAffected":   rowsAffected,
	}, nil
}

func (a *Activity) updateCollection(ctx activity.Context, txn sqlbuilder.Tx, collection string, mapping map[string]interface{}, data map[string]interface{}) (map[string]interface{}, error) {
	columns, values := a.mapDbFields(ctx, mapping, data)
	if len(values) == 0 {
		return nil, fmt.Errorf("Cannot map any value")
	}
	pos := funk.IndexOf(columns, "id")
	ctx.Logger().Debugf("columns: %v", columns)
	ctx.Logger().Debugf("pos: %v", pos)
	if pos >= 0 {
		updater := txn.Update(collection)
		ctx.Logger().Debugf("len(columns): %v", len(columns))
		for i := 0; i < len(columns); i++ {
			if columns[i] != "id" {
				updater = updater.Set(fmt.Sprintf("%v = ?", columns[i]), values[i])
			} else if columns[i] == "id" {
				updater = updater.Where(fmt.Sprintf("%v = ?", columns[i]), values[i])
			}
		}
		res, err := updater.Exec()
		if err != nil {
			return nil, err
		}
		rowsAffected, _ := res.RowsAffected()
		return map[string]interface{}{
			"lastUpdatedID": values[pos],
			"rowsAffected":  rowsAffected,
		}, nil
	}
	return nil, fmt.Errorf("id field not found")
}

func (a *Activity) deleteCollection(ctx activity.Context, txn sqlbuilder.Tx, collection string, mapping map[string]interface{}, data map[string]interface{}) (map[string]interface{}, error) {
	columns, values := a.mapDbFields(ctx, mapping, data)
	if len(values) == 0 {
		return nil, fmt.Errorf("Cannot map any value")
	}
	pos := funk.IndexOf(columns, "id")
	if pos >= 0 {
		res, err := txn.DeleteFrom(collection).Where("id = ?", values[pos]).Exec()
		if err != nil {
			return nil, err
		}
		rowsAffected, _ := res.RowsAffected()
		return map[string]interface{}{
			"lastDeletedID": values[pos],
			"rowsDeleted":   rowsAffected,
		}, nil
	}
	return nil, fmt.Errorf("id field not found")
}

func (a *Activity) upsertCollection(ctx activity.Context, txn sqlbuilder.Tx, collection string, mapping map[string]interface{}, data map[string]interface{}) (map[string]interface{}, error) {
	columns, values := a.mapDbFields(ctx, mapping, data)
	if len(values) == 0 {
		return nil, fmt.Errorf("Cannot map any value")
	}
	pos := funk.IndexOf(columns, "id")
	if pos >= 0 {
		return a.updateCollection(ctx, txn, collection, mapping, data)
	}
	return a.insertCollection(ctx, txn, collection, mapping, data)
}

func (a *Activity) mapDbFields(ctx activity.Context, mapping map[string]interface{}, data map[string]interface{}) ([]string, []interface{}) {
	var err error

	columns := make([]string, 0)
	values := make([]interface{}, 0)

	for k, v := range mapping {
		var value interface{}
		value, err = jsonpath.JsonPathLookup(data, v.(string))
		if err != nil {
			ctx.Logger().Debugf("jsonPath: %v, data: %v", v, data)
			ctx.Logger().Warnf("Json path %v not found or invalid", v)
		} else {
			columns = append(columns, k)
			values = append(values, value)
		}
	}

	pos := funk.IndexOf(columns, "id")
	ctx.Logger().Debugf("IndexOf(columns): %v", pos)
	if pos == -1 {
		columns = append(columns, "id")
		uuidVar := uuid.New()
		values = append(values, fmt.Sprintf("%v", uuidVar))
	}

	return columns, values
}

func mapOptionsToString(sOptions map[string]interface{}) map[string]string {
	options := make(map[string]string)
	for k, v := range sOptions {
		value := v.(string)
		if len(value) > 0 {
			options[k] = v.(string)
		}
	}
	return options
}
