package main

type Tag struct {
	Sequence uint32
	Client uint32
}

func (tag Tag) Less(other Tag) bool {
	return other.Sequence > tag.Sequence || (other.Sequence == tag.Sequence && other.Client > tag.Client)
}

type SortableTags []Tag

func (tags SortableTags) Len() int {
	return len(tags)
}

func (tags SortableTags) Less(i, j int) bool {
	return tags[i].Less(tags[j])
}

func (tags SortableTags) Swap(i, j int) {
	tmp := tags[i]
	tags[i] = tags[j]
	tags[j] = tmp
}
