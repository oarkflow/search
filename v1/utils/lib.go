package utils

func Intersect(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range a {
		m[id] = true
	}
	var result []int
	for _, id := range b {
		if m[id] {
			result = append(result, id)
		}
	}
	return result
}

func Union(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range a {
		m[id] = true
	}
	for _, id := range b {
		m[id] = true
	}
	var result []int
	for id := range m {
		result = append(result, id)
	}
	return result
}

func Subtract(a, b []int) []int {
	m := make(map[int]bool)
	for _, id := range b {
		m[id] = true
	}
	var result []int
	for _, id := range a {
		if !m[id] {
			result = append(result, id)
		}
	}
	return result
}

func Abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
