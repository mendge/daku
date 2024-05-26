package response

import (
	"github.com/mendge/daku/internal/grep"
	"github.com/mendge/daku/internal/persistence"
	"github.com/mendge/daku/service/frontend/models"
	"github.com/samber/lo"
)

func ToSearchDAGsResponse(ret []*persistence.GrepResult, errs []string) *models.SearchDagsResponse {
	return &models.SearchDagsResponse{
		Results: lo.Map(ret, func(item *persistence.GrepResult, _ int) *models.SearchDagsResultItem {
			return ToSearchDAGsResultItem(item)
		}),
		Errors: errs,
	}
}

func ToSearchDAGsResultItem(workflow *persistence.GrepResult) *models.SearchDagsResultItem {
	return &models.SearchDagsResultItem{
		Name: workflow.Name,
		DAG:  ToDAG(workflow.DAG),
		Matches: lo.Map(workflow.Matches, func(item *grep.Match, _ int) *models.SearchDagsMatchItem {
			return ToSearchDAGsMatchItem(item)
		}),
	}
}

func ToSearchDAGsMatchItem(match *grep.Match) *models.SearchDagsMatchItem {
	return &models.SearchDagsMatchItem{
		Line:       match.Line,
		LineNumber: int64(match.LineNumber),
		StartLine:  int64(match.StartLine),
	}
}
