export type ItineraryItem = {
  name?: string
  type?: string
  source_tool_call_ids?: string[]
}

export type ItineraryContent = {
  destination?: string
  days?: Array<{
    day?: number
    items?: ItineraryItem[]
  }>
}

export function itineraryItems(content?: ItineraryContent): ItineraryItem[] {
  return (content?.days ?? []).flatMap((day) => day.items ?? [])
}
