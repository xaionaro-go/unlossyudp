package fecio

type WriterOpt interface {
	apply(*writerConfig)
}

type WriterOpts []WriterOpt

func (s WriterOpts) apply(cfg *writerConfig) {
	for _, opt := range s {
		opt.apply(cfg)
	}
}

func (s WriterOpts) config() writerConfig {
	cfg := writerConfig{}
	s.apply(&cfg)
	return cfg
}

type writerConfig struct {
	NoCopy bool
}

type WriterOptUnsafeNoCopy bool

func (opt WriterOptUnsafeNoCopy) apply(cfg *writerConfig) {
	cfg.NoCopy = bool(opt)
}
