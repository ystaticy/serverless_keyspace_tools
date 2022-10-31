package placement

const (

	// DCLabelKey indicates the key of label which represents the dc for Store.
	// FIXME: currently we assumes "zone" is the dcLabel key in Store
	DCLabelKey = "zone"
	// EngineLabelKey is the label that indicates the backend of store instance:
	// tikv or tiflash. TiFlash instance will contain a label of 'engine: tiflash'.
	EngineLabelKey = "engine"
	// EngineLabelTiFlash is the label value, which a TiFlash instance will have with
	// a label key of EngineLabelKey.
	EngineLabelTiFlash = "tiflash"
	// EngineLabelTiKV is the label value used in some tests. And possibly TiKV will
	// set the engine label with a value of EngineLabelTiKV.
	EngineLabelTiKV = "tikv"
)
