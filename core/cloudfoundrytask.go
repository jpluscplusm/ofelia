package core

import (
	"fmt"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-community/go-cfenv"
	"github.com/jpillora/backoff"
	"net/url"
	"time"
)

type CloudFoundryTask struct {
	BareJob
	Client              *cfclient.Client
	Env                 *cfenv.App
	CloudFoundryAppName string `gcfg:"cf-appname"`
}

func NewCloudFoundryTask(c *cfclient.Client, a *cfenv.App) *CloudFoundryTask {
	return &CloudFoundryTask{
		Client: c,
		Env:    a,
	}
}

func BuildCloudFoundryContext() (*cfclient.Client, *cfenv.App, error) {
	myEnv, err := cfenv.Current()
	if err != nil {
		return nil, nil, err
	}
	login, err := myEnv.Services.WithName("scheduler-cf-login")
	if err != nil {
		return nil, nil, fmt.Errorf("'scheduler-cf-login' service not found")
	}
	c := &cfclient.Config{
		ApiAddress: myEnv.CFAPI,
		Username:   login.Credentials["username"].(string),
		Password:   login.Credentials["password"].(string),
	}
	client, err := cfclient.NewClient(c)
	if err != nil {
		return nil, nil, err
	}
	return client, myEnv, nil
}

func (cft *CloudFoundryTask) Run(ctx *Context) error {
	task, err := cft.createTask()
	if err != nil {
		return err
	}

	delay := &backoff.Backoff{
		Min:    2 * time.Second,
		Max:    10 * time.Second,
		Factor: 1.1,
		Jitter: true,
	}

	for {
		time.Sleep(delay.Duration())
		task, err = cft.Client.GetTaskByGuid(task.GUID)
		if err != nil {
			return err
		}
		switch task.State {
		case "RUNNING": // loop round again after sleeping
		case "SUCCEEDED":
			return nil
		case "FAILED":
			return fmt.Errorf("task failed")
		default:
			return fmt.Errorf("task state unknown: %s", task.State)
		}
	}
	return fmt.Errorf("not really sure how we got here ...")
}

func (cft *CloudFoundryTask) appGuid() (string, error) {
	v := url.Values{}
	v.Set("q", fmt.Sprintf("name:%s", cft.CloudFoundryAppName))
	v.Add("q", fmt.Sprintf("space_guid:%s", cft.Env.SpaceID))
	app, err := cft.Client.ListAppsByQuery(v)
	if err != nil {
		return "", err
	}
	switch x := len(app); {
	case x == 1: // all is good - log something?
	case x == 0: // app not found
		return "", fmt.Errorf("app '%s' not found in space '%s'", cft.CloudFoundryAppName, cft.Env.SpaceName)
	case x > 1: // something odd happened - maybe our ListAppsByQuery() *wasn't* space-scoped?
		return "", fmt.Errorf("app '%s' not unique in space '%s'", cft.CloudFoundryAppName, cft.Env.SpaceName)
	}
	return app[0].Guid, nil
}

func (cft *CloudFoundryTask) createTask() (task cfclient.Task, err error) {
	appGuid, err := cft.appGuid()
	if err != nil {
		return task, err
	}
	task, err = cft.Client.CreateTask(cfclient.TaskRequest{
		Name:        cft.Name,
		Command:     cft.Command,
		DropletGUID: appGuid,
	})
	return task, err
}
