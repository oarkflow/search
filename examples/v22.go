package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/oarkflow/search/v1"
)

// messages for indexing progress
type indexedMsg struct{ current, total int }
type indexDoneMsg struct{ index *v1.InvertedIndex }

// debounced search message
type searchMsg string

type model struct {
	// indexing state
	indexing   bool
	spinner    spinner.Model
	current    int
	total      int
	progressCh chan tea.Msg

	// search input
	textInput    textinput.Model
	pendingQuery string

	// results table
	table       table.Model
	index       *v1.InvertedIndex
	results     []v1.ScoredDoc
	currentPage int
	pageSize    int
}

func initialModel(jsonPath string) model {
	sp := spinner.New()
	sp.Spinner = spinner.Dot

	ti := textinput.New()
	ti.Placeholder = "Type to search (min 3 chars)..."

	columns := []table.Column{
		{Title: "DocID", Width: 6},
		{Title: "Score", Width: 7},
		{Title: "Data", Width: 50},
	}
	tbl := table.New(table.WithColumns(columns))
	tbl.SetStyles(table.DefaultStyles())
	tbl.SetHeight(10)

	m := model{
		indexing:   true,
		spinner:    sp,
		progressCh: make(chan tea.Msg, 1),
		textInput:  ti,
		table:      tbl,
		pageSize:   10,
	}
	go runIndexing(jsonPath, m.progressCh)
	return m
}

func runIndexing(path string, progressCh chan tea.Msg) {
	total, err := v1.RowCount(path)
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	var count int
	idx, err := v1.BuildIndex(path, func(v v1.GenericRecord) error {
		count++
		if count%100000 == 0 {
			select {
			case progressCh <- indexedMsg{current: count, total: total}:
			default:
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error building index: %v", err)
	}
	// final progress update
	progressCh <- indexedMsg{current: total, total: total}
	progressCh <- indexDoneMsg{index: idx}
}

func nextProgressMsg(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

func debounceSearch(q string) tea.Cmd {
	return tea.Tick(time.Millisecond*200, func(t time.Time) tea.Msg {
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

func (m model) Init() tea.Cmd {
	return tea.Batch(
		spinner.Tick,
		nextProgressMsg(m.progressCh),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// global quit
	if key, ok := msg.(tea.KeyMsg); ok {
		if key.Type == tea.KeyCtrlC || key.String() == "q" {
			return m, tea.Quit
		}
	}

	switch msg := msg.(type) {
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
		m.index = msg.index
		m.textInput.Focus()
		return m, nil

	case searchMsg:
		// perform actual search
		q := string(msg)
		scored := v1.ScoreQuery(v1.NewTermQuery(q, true, 1), m.index, q)
		m.results = scored
		m.currentPage = 0
		rows := []table.Row{}
		for _, sd := range paginate(scored, 0, m.pageSize) {
			rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
		}
		m.table.SetRows(rows)
		return m, nil

	case tea.KeyMsg:
		if !m.indexing {
			switch msg.String() {
			case "n":
				if (m.currentPage+1)*m.pageSize < len(m.results) {
					m.currentPage++
					rows := []table.Row{}
					for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
						rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
					}
					m.table.SetRows(rows)
				}
				return m, nil
			case "p":
				if m.currentPage > 0 {
					m.currentPage--
					rows := []table.Row{}
					for _, sd := range paginate(m.results, m.currentPage, m.pageSize) {
						rows = append(rows, table.Row{fmt.Sprint(sd.DocID), fmt.Sprintf("%.4f", sd.Score), fmt.Sprint(m.index.Documents[sd.DocID])})
					}
					m.table.SetRows(rows)
				}
				return m, nil
			}
		}
		// pass other keys to text input
		var cmd tea.Cmd
		m.textInput, cmd = m.textInput.Update(msg)
		if !m.indexing {
			q := m.textInput.Value()
			if len(q) >= 3 && q != m.pendingQuery {
				m.pendingQuery = q
				return m, debounceSearch(q)
			}
			if len(q) < 3 {
				m.results = nil
				m.table.SetRows(nil)
			}
		}
		return m, cmd

	default:
		return m, nil
	}
}

func (m model) View() string {
	searchUI := ""
	if m.index != nil {
		searchUI = fmt.Sprintf("%s\n\n%s", m.textInput.View(), m.table.View())
	}

	progress := ""
	if m.indexing {
		progress = fmt.Sprintf("Indexing %d/%d %s", m.current, m.total, m.spinner.View())
	} else {
		progress = fmt.Sprintf("Indexing complete: %d records indexed.", m.total)
	}

	pageInfo := ""
	if len(m.results) > 0 {
		totalPages := int(math.Ceil(float64(len(m.results)) / float64(m.pageSize)))
		pageInfo = fmt.Sprintf("Page %d/%d | (n) Next | (p) Prev | (q) Quit", m.currentPage+1, totalPages)
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
