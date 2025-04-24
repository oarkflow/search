// Filename: main.go
package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	v1 "github.com/oarkflow/search/v1"
	"github.com/oarkflow/search/v1/utils"
)

type indexedMsg struct{ current, total int }
type indexDoneMsg struct{ index *v1.InvertedIndex }

// noopMsg used for non-blocking polling
type noopMsg struct{}
type searchMsg string

type model struct {
	indexing    bool
	spinner     spinner.Model
	current     int
	total       int
	progressCh  chan tea.Msg
	textInput   textinput.Model
	table       table.Model
	index       *v1.InvertedIndex
	results     []v1.ScoredDoc
	currentPage int
	pageSize    int
}

func customTableStyles() table.Styles {
	s := table.DefaultStyles()
	border := lipgloss.NormalBorder()
	s.Header.Border(border)
	s.Cell.Border(border)
	s.Selected.Border(border)
	return s
}

func initialModel(jsonPath string) *model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot
	ti := textinput.New()
	ti.Width = 100
	ti.Placeholder = "Type to search..."
	ti.Focus()
	columns := []table.Column{
		{Title: "DocID", Width: 6},
		{Title: "Score", Width: 7},
		{Title: "Data", Width: 100},
	}
	tbl := table.New(
		table.WithColumns(columns),
		table.WithStyles(customTableStyles()),
		table.WithHeight(10),
	)
	m := &model{
		index:      v1.NewIndex(),
		indexing:   true,
		spinner:    sp,
		progressCh: make(chan tea.Msg, 1),
		textInput:  ti,
		table:      tbl,
		pageSize:   10,
	}
	// start indexing in background
	go m.runIndexing(jsonPath)
	return m
}

func (m *model) runIndexing(path string) {
	total, err := utils.RowCount(path)
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	var count int
	err = m.index.Build(path, func(rec v1.GenericRecord) error {
		count++
		if count%100000 == 0 || count == total {
			// non-blocking send
			select {
			case m.progressCh <- indexedMsg{current: count, total: total}:
			default:
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	// final updates
	m.progressCh <- indexedMsg{current: count, total: total}
	m.progressCh <- indexDoneMsg{index: m.index}
}

// nextProgressMsg polls progressCh non-blockingly
func nextProgressMsg(ch chan tea.Msg) tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
		select {
		case msg := <-ch:
			return msg
		default:
			return noopMsg{}
		}
	})
}

func debounceSearch(q string) tea.Cmd {
	return tea.Tick(time.Millisecond*200, func(_ time.Time) tea.Msg {
		return searchMsg(q)
	})
}

func paginate(results []v1.ScoredDoc, page, size int) []v1.ScoredDoc {
	start := page * size
	if start >= len(results) {
		return nil
	}
	end := int(math.Min(float64(start+size), float64(len(results))))
	return results[start:end]
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(
		spinner.Tick,
		nextProgressMsg(m.progressCh),
	)
}

func mapToTable(data []v1.GenericRecord) ([]table.Column, []table.Row) {
	if len(data) == 0 {
		return nil, nil
	}
	// collect keys and max widths
	keys := make([]string, 0, len(data[0]))
	for key := range data[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	colWidths := make(map[string]int)
	for _, key := range keys {
		colWidths[key] = len(key)
	}
	for _, rowMap := range data {
		for _, key := range keys {
			valStr := fmt.Sprintf("%v", rowMap[key])
			if len(valStr) > colWidths[key] {
				colWidths[key] = len(valStr)
			}
		}
	}
	var rows []table.Row
	for _, rowMap := range data {
		var row table.Row
		for _, key := range keys {
			row = append(row, fmt.Sprintf("%v", rowMap[key]))
		}
		rows = append(rows, row)
	}
	var columns []table.Column
	for _, key := range keys {
		columns = append(columns, table.Column{Title: key, Width: colWidths[key] + 1})
	}
	return columns, rows
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyCtrlRight:
			if len(m.results) > 0 && (m.currentPage+1)*m.pageSize < len(m.results) {
				m.currentPage++
				var recs []v1.GenericRecord
				for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
					recs = append(recs, m.index.Documents[sd.DocID])
				}
				cols, rows := mapToTable(recs)
				m.table.SetColumns(cols)
				m.table.SetRows(rows)
			}
			return m, nil
		case tea.KeyCtrlLeft:
			if m.currentPage > 0 {
				m.currentPage--
				var recs []v1.GenericRecord
				for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
					recs = append(recs, m.index.Documents[sd.DocID])
				}
				cols, rows := mapToTable(recs)
				m.table.SetColumns(cols)
				m.table.SetRows(rows)
			}
			return m, nil
		}
		// text input update and debounce
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		query := strings.TrimSpace(m.textInput.Value())
		if len(query) >= 1 {
			return m, tea.Batch(cmd, debounceSearch(query))
		}
		m.results = nil
		m.table.SetRows(nil)
		return m, cmd

	case spinner.TickMsg:
		if m.indexing {
			m.spinner, _ = m.spinner.Update(msg)
			return m, tea.Batch(spinner.Tick, nextProgressMsg(m.progressCh))
		}
		return m, nil

	case indexedMsg:
		m.current = msg.current
		m.total = msg.total
		return m, nextProgressMsg(m.progressCh)

	case indexDoneMsg:
		m.indexing = false
		return m, nil

	case searchMsg:
		query := strings.TrimSpace(m.textInput.Value())
		if string(msg) != query || len(query) == 0 {
			return m, nil
		}
		m.results = m.index.Search(v1.NewTermQuery(query, true, 1), query)
		m.currentPage = 0
		// build initial page
		if recs := paginate(m.results, 0, m.pageSize); len(recs) > 0 {
			var data []v1.GenericRecord
			for _, sd := range recs {
				data = append(data, m.index.Documents[sd.DocID])
			}
			cols, rows := mapToTable(data)
			m.table.SetColumns(cols)
			m.table.SetRows(rows)
		} else {
			m.table.SetRows(nil)
		}
		return m, nil

	case noopMsg:
		if m.indexing {
			return m, nextProgressMsg(m.progressCh)
		}
		return m, nil

	default:
		return m, nil
	}
}

func (m *model) View() string {
	searchUI := fmt.Sprintf("%s\n\n%s", m.textInput.View(), m.table.View())
	var progress string
	if m.indexing {
		progress = fmt.Sprintf("Indexing %d/%d %s", m.current, m.total, m.spinner.View())
	} else {
		progress = fmt.Sprintf("Indexing complete: %d records indexed.", m.total)
	}
	var pageInfo string
	if len(m.results) > 0 {
		totalPages := int(math.Ceil(float64(len(m.results)) / float64(m.pageSize)))
		pageInfo = fmt.Sprintf("Use Ctrl+→ / Ctrl+← to navigate pages. Page %d/%d. Ctrl+C to quit.", m.currentPage+1, totalPages)
	} else {
		pageInfo = "Type at least 1 character to search. Ctrl+C to quit."
	}
	return fmt.Sprintf("%s\n\n%s\n\n%s", searchUI, progress, pageInfo)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: search <json-file>")
		os.Exit(1)
	}
	jsonPath := os.Args[1]
	p := tea.NewProgram(initialModel(jsonPath), tea.WithAltScreen())
	if err := p.Start(); err != nil {
		log.Fatalf("Error running program: %v", err)
	}
}
