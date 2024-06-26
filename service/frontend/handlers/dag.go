package handlers

import (
	"errors"
	"fmt"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"github.com/mendge/daku/internal/constants"
	"github.com/mendge/daku/internal/dag"
	"github.com/mendge/daku/internal/engine"
	"github.com/mendge/daku/internal/etcd/estore"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/internal/persistence/jsondb"
	domain "github.com/mendge/daku/internal/persistence/model"
	"github.com/mendge/daku/internal/scheduler"
	"github.com/mendge/daku/service/frontend/handlers/response"
	"github.com/mendge/daku/service/frontend/models"
	"github.com/mendge/daku/service/frontend/restapi/operations"
	"github.com/mendge/daku/service/frontend/server"
	"github.com/samber/lo"
)

const (
	// TODO: separate API
	dagTabTypeStatus       = "status"
	dagTabTypeSpec         = "spec"
	dagTabTypeHistory      = "history"
	dagTabTypeStepLog      = "log"
	dagTabTypeSchedulerLog = "scheduler-log"
)

var (
	errInvalidArgs = errors.New("invalid argument")
)

type DAGHandler struct {
	engineFactory engine.Factory
}

func NewDAG(engineFactory engine.Factory) server.New {
	return &DAGHandler{
		engineFactory: engineFactory,
	}
}

func (h *DAGHandler) Configure(api *operations.DaguAPI) {
	// done
	api.ListDagsHandler = operations.ListDagsHandlerFunc(
		func(params operations.ListDagsParams) middleware.Responder {
			resp, err := h.GetList(params)
			if err != nil {
				return operations.NewListDagsDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewListDagsOK().WithPayload(resp)
		})
	// done
	api.GetDagDetailsHandler = operations.GetDagDetailsHandlerFunc(
		func(params operations.GetDagDetailsParams) middleware.Responder {
			resp, err := h.GetDetail(params)
			if err != nil {
				return operations.NewGetDagDetailsDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewGetDagDetailsOK().WithPayload(resp)
		})

	api.PostDagActionHandler = operations.PostDagActionHandlerFunc(
		func(params operations.PostDagActionParams) middleware.Responder {
			resp, err := h.PostAction(params)
			if err != nil {
				return operations.NewPostDagActionDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewPostDagActionOK().WithPayload(resp)
		})
	// done
	api.CreateDagHandler = operations.CreateDagHandlerFunc(
		func(params operations.CreateDagParams) middleware.Responder {
			resp, err := h.Create(params)
			if err != nil {
				return operations.NewCreateDagDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewCreateDagOK().WithPayload(resp)
		})

	api.DeleteDagHandler = operations.DeleteDagHandlerFunc(
		func(params operations.DeleteDagParams) middleware.Responder {
			err := h.Delete(params)
			if err != nil {
				return operations.NewDeleteDagDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewDeleteDagOK()
		})

	api.SearchDagsHandler = operations.SearchDagsHandlerFunc(
		func(params operations.SearchDagsParams) middleware.Responder {
			resp, err := h.Search(params)
			if err != nil {
				return operations.NewSearchDagsDefault(err.Code).WithPayload(err.APIError)
			}
			return operations.NewSearchDagsOK().WithPayload(resp)
		})
}

func (h *DAGHandler) Create(params operations.CreateDagParams) (*models.CreateDagResponse, *response.CodedError) {
	switch lo.FromPtr(params.Body.Action) {
	case "new":
		name := *params.Body.Value
		e := h.engineFactory.Create()
		id, err := e.CreateDAG(name)
		if err != nil {
			return nil, response.NewInternalError(err)
		}
		return &models.CreateDagResponse{DagID: swag.String(id)}, nil
	default:
		return nil, response.NewBadRequestError(errInvalidArgs)
	}
}
func (h *DAGHandler) Delete(params operations.DeleteDagParams) *response.CodedError {
	e := h.engineFactory.Create()
	dagStatus, err := e.GetStatus(params.DagID)
	if err != nil {
		return response.NewNotFoundError(err)
	}
	if err := e.DeleteDAG(params.DagID, dagStatus.DAG.Location); err != nil {
		return response.NewInternalError(err)
	}
	return nil
}

func (h *DAGHandler) GetList(_ operations.ListDagsParams) (*models.ListDagsResponse, *response.CodedError) {
	e := h.engineFactory.Create()
	dags, errs, err := e.GetAllStatus()
	if err != nil {
		return nil, response.NewInternalError(err)
	}

	// TODO: remove this if it's not needed
	_, _, hasErr := lo.FindIndexOf(dags, func(d *persistence.DAGStatus) bool {
		return d.Error != nil
	})

	if len(errs) > 0 {
		hasErr = true
	}

	return response.ToListDagResponse(dags, errs, hasErr), nil
}

func (h *DAGHandler) GetDetail(params operations.GetDagDetailsParams) (*models.GetDagDetailsResponse, *response.CodedError) {
	dagID := params.DagID

	// TODO: separate API
	tab := dagTabTypeStatus
	if params.Tab != nil {
		tab = *params.Tab
	}

	logFile := params.File
	stepName := params.Step

	e := h.engineFactory.Create()
	dagStatus, err := e.GetStatus(dagID)
	if dagStatus == nil {
		return nil, response.NewNotFoundError(err)
	}

	resp := response.ToGetDagDetailResponse(
		dagStatus,
		tab,
	)

	if err != nil {
		resp.Errors = append(resp.Errors, err.Error())
	}

	switch tab {
	case dagTabTypeStatus:
	case dagTabTypeSpec:
		dagContent, err := e.GetDAGSpec(dagID)
		if err != nil {
			return nil, response.NewNotFoundError(err)
		}
		resp.Definition = lo.ToPtr(dagContent)

	case dagTabTypeHistory:
		e := h.engineFactory.Create()
		logs := e.GetRecentHistory(dagStatus.DAG, 30)
		resp.LogData = response.ToDagLogResponse(logs)

	case dagTabTypeStepLog:
		stepLog, err := h.getStepLog(dagStatus.DAG, lo.FromPtr(logFile), lo.FromPtr(stepName))
		if err != nil {
			return nil, response.NewNotFoundError(err)
		}
		resp.StepLog = stepLog

	case dagTabTypeSchedulerLog:
		schedulerLog, err := h.readSchedulerLog(dagStatus.DAG, lo.FromPtr(logFile))
		if err != nil {
			return nil, response.NewNotFoundError(err)
		}
		resp.ScLog = schedulerLog

	default:
	}

	return resp, nil
}

func (h *DAGHandler) getStepLog(dag *dag.DAG, logFile, stepName string) (*models.DagStepLogResponse, error) {
	var stepByName = map[string]*domain.Node{
		constants.OnSuccess: nil,
		constants.OnFailure: nil,
		constants.OnCancel:  nil,
		constants.OnExit:    nil,
	}

	var status *domain.Status

	e := h.engineFactory.Create()

	if logFile == "" {
		s, err := e.GetLatestStatus(dag)
		if err != nil {
			return nil, fmt.Errorf("failed to read status")
		}
		status = s
	} else {
		// TODO: fix not to use json db directly
		s, err := jsondb.ParseFile(logFile)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", logFile, err)
		}
		status = s
	}

	stepByName[constants.OnSuccess] = status.OnSuccess
	stepByName[constants.OnFailure] = status.OnFailure
	stepByName[constants.OnCancel] = status.OnCancel
	stepByName[constants.OnExit] = status.OnExit

	node, ok := lo.Find(status.Nodes, func(item *domain.Node) bool {
		return item.Name == stepName
	})
	if !ok {
		return nil, fmt.Errorf("step name was not found %s", stepName)
	}

	logContent, err := estore.GetContentOfFile(node.Log)
	if err != nil {
		return nil, fmt.Errorf("error reading %s: %w", node.Log, err)
	}

	return response.ToDagStepLogResponse(node.Log, string(logContent), node), nil
}

func (h *DAGHandler) readSchedulerLog(dag *dag.DAG, statusFile string) (*models.DagSchedulerLogResponse, error) {
	var logFile string

	e := h.engineFactory.Create()

	if statusFile == "" {
		s, err := e.GetLatestStatus(dag)
		if err != nil {
			return nil, fmt.Errorf("error reading the last status")
		}
		logFile = s.Log
	} else {
		// TODO: fix not to use json db directly
		s, err := jsondb.ParseFile(statusFile)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", statusFile, err)
		}
		logFile = s.Log
	}

	content, err := estore.GetContentOfFile(logFile)
	if err != nil {
		return nil, fmt.Errorf("error reading %s: %w", logFile, err)
	}

	return response.ToDagSchedulerLogResponse(logFile, string(content)), nil
}

func (h *DAGHandler) PostAction(params operations.PostDagActionParams) (*models.PostDagActionResponse, *response.CodedError) {
	e := h.engineFactory.Create()
	d, err := e.GetStatus(params.DagID)
	// ??
	if err != nil && *params.Body.Action != "save" {
		return nil, response.NewBadRequestError(err)
	}

	switch *params.Body.Action {
	case "start":
		if d.Status.Status == scheduler.SchedulerStatus_Running {
			return nil, response.NewBadRequestError(errInvalidArgs)
		}
		e := h.engineFactory.Create()
		e.StartAsync(d.DAG, params.Body.Params)

	case "suspend":
		_ = e.ToggleSuspend(params.DagID, params.Body.Value == "true")

	case "stop":
		if d.Status.Status != scheduler.SchedulerStatus_Running {
			return nil, response.NewBadRequestError(fmt.Errorf("the DAG is not running: %w", errInvalidArgs))
		}
		e := h.engineFactory.Create()
		if err := e.Stop(d.DAG); err != nil {
			return nil, response.NewBadRequestError(fmt.Errorf("error trying to stop the DAG: %w", err))
		}

	case "retry":
		if params.Body.RequestID == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("request-id is required: %w", errInvalidArgs))
		}
		e := h.engineFactory.Create()
		err = e.Retry(d.DAG, params.Body.RequestID)
		if err != nil {
			return nil, response.NewInternalError(fmt.Errorf("error trying to retry the DAG: %w", err))
		}

	case "mark-success":
		if d.Status.Status == scheduler.SchedulerStatus_Running {
			return nil, response.NewBadRequestError(fmt.Errorf("the DAG is still running: %w", errInvalidArgs))
		}
		if params.Body.RequestID == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("request-id is required: %w", errInvalidArgs))
		}
		if params.Body.Step == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("step name is required: %w", errInvalidArgs))
		}

		err = h.updateStatus(d.DAG, params.Body.RequestID, params.Body.Step, scheduler.NodeStatus_Success)
		if err != nil {
			return nil, response.NewInternalError(err)
		}

	case "mark-failed":
		if d.Status.Status == scheduler.SchedulerStatus_Running {
			return nil, response.NewBadRequestError(fmt.Errorf("the DAG is still running: %w", errInvalidArgs))
		}
		if params.Body.RequestID == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("request-id is required: %w", errInvalidArgs))
		}
		if params.Body.Step == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("step name is required: %w", errInvalidArgs))
		}

		err = h.updateStatus(d.DAG, params.Body.RequestID, params.Body.Step, scheduler.NodeStatus_Error)
		if err != nil {
			return nil, response.NewInternalError(err)
		}

	case "save":
		e := h.engineFactory.Create()
		err := e.UpdateDAG(params.DagID, params.Body.Value)
		if err != nil {
			return nil, response.NewInternalError(err)
		}

	case "rename":
		newName := params.Body.Value
		if newName == "" {
			return nil, response.NewBadRequestError(fmt.Errorf("new name is required: %w", errInvalidArgs))
		}
		e := h.engineFactory.Create()
		if err := e.Rename(params.DagID, newName); err != nil {
			return nil, response.NewInternalError(err)
		}
		return &models.PostDagActionResponse{NewDagID: params.Body.Value}, nil

	default:
		return nil, response.NewBadRequestError(fmt.Errorf("invalid action: %s", *params.Body.Action))
	}

	return &models.PostDagActionResponse{}, nil
}

func (h *DAGHandler) updateStatus(dag *dag.DAG, reqId, step string, to scheduler.NodeStatus) error {
	e := h.engineFactory.Create()
	status, err := e.GetStatusByRequestId(dag, reqId)
	if err != nil {
		return err
	}

	_, idx, ok := lo.FindIndexOf(status.Nodes, func(item *domain.Node) bool {
		return item.Step.Name == step
	})
	if !ok {
		return fmt.Errorf("step was not found: %s", step)
	}

	status.Nodes[idx].Status = to
	status.Nodes[idx].StatusText = to.String()

	return e.UpdateStatus(dag, status)
}

func (h *DAGHandler) Search(params operations.SearchDagsParams) (*models.SearchDagsResponse, *response.CodedError) {
	query := params.Q
	if query == "" {
		return nil, response.NewBadRequestError(errInvalidArgs)
	}

	e := h.engineFactory.Create()
	ret, errs, err := e.Grep(query)
	if err != nil {
		return nil, response.NewInternalError(err)
	}

	return response.ToSearchDAGsResponse(ret, errs), nil
}

// getLogFileContent & readFileContent 被删除, 因为默认从etcd读到的日志文件都是utf-8
