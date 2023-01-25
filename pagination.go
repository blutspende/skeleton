package skeleton

import (
	"encoding/json"
	"time"
)

type SortDirection string // @Name SortDirection

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
	SortNone SortDirection = ""
)

func (s SortDirection) String() string {
	return string(s)
}

type Filter struct {
	TimeFrom *time.Time `form:"timeFrom" json:"timeFrom"`
	Filter   *string    `form:"filter" json:"filter"`
	Pageable Pageable
}

type Pageable struct {
	Page      int           `form:"page,default=0" json:"page" validate:"required" minimum:"0" default:"0"`           // The desired page number
	PageSize  int           `form:"pageSize,default=25" json:"pageSize" validate:"required" minimum:"0" default:"25"` // The desired number of items per page
	Sort      string        `form:"sort" json:"sort"`                                                                 // The sorting parameter
	Direction SortDirection `form:"direction" json:"direction" enums:"asc,desc"`                                      // The sorting direction
} // @Name Pageable

func (p *Pageable) UnmarshalJSON(data []byte) error {
	pageable := Pageable{
		Page:     0,
		PageSize: 25,
	}
	if err := json.Unmarshal(data, &pageable); err != nil {
		return err
	}
	*p = pageable
	return nil
}

func (p *Pageable) IsPaged() bool {
	return p.PageSize > 0
}

func (p *Pageable) IsUnPaged() bool {
	return p.PageSize == 0
}

type Page struct {
	Items      interface{} `json:"content"`                 // The items
	Page       int         `json:"currentPage" example:"1"` // The actual page number
	PageSize   int         `json:"pageSize" example:"50"`   // The number of items per page
	TotalCount int         `json:"totalCount" example:"69"` // The total count of items
	TotalPages int         `json:"totalPages" example:"2"`  // The total pages
} // @Name Page

func NewPage(pageable Pageable, totalCount int, items interface{}) Page {
	var totalPages int
	if pageable.IsUnPaged() {
		if totalCount > 0 {
			totalPages = 1
		}

		return Page{
			Items:      items,
			Page:       0,
			PageSize:   0,
			TotalCount: totalCount,
			TotalPages: totalPages,
		}
	}

	if totalCount > 0 {
		totalPages = (totalCount + pageable.PageSize - 1) / pageable.PageSize
	}

	return Page{
		Items:      items,
		Page:       pageable.Page,
		PageSize:   pageable.PageSize,
		TotalCount: totalCount,
		TotalPages: totalPages,
	}
}
