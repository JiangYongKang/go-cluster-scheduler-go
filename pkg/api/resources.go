package api

// Add returns a+b.
func Add(a, b Resources) Resources {
	return Resources{
		MilliCPU: a.MilliCPU + b.MilliCPU,
		MemoryMB: a.MemoryMB + b.MemoryMB,
		GPUCount: a.GPUCount + b.GPUCount,
	}
}

// Sub returns a-b.
func Sub(a, b Resources) Resources {
	return Resources{
		MilliCPU: a.MilliCPU - b.MilliCPU,
		MemoryMB: a.MemoryMB - b.MemoryMB,
		GPUCount: a.GPUCount - b.GPUCount,
	}
}

// LessEqual reports whether every dimension of a is <= the same dimension of b.
func LessEqual(a, b Resources) bool {
	return a.MilliCPU <= b.MilliCPU && a.MemoryMB <= b.MemoryMB && a.GPUCount <= b.GPUCount
}

// Scale returns r*n (used for resource-time accrual).
func Scale(r Resources, n int64) Resources {
	return Resources{
		MilliCPU: r.MilliCPU * n,
		MemoryMB: r.MemoryMB * n,
		GPUCount: r.GPUCount * n,
	}
}

// IsZero reports whether r is the zero vector.
func (r Resources) IsZero() bool {
	return r.MilliCPU == 0 && r.MemoryMB == 0 && r.GPUCount == 0
}
