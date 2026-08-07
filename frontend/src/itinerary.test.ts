import { describe, expect, it } from 'vitest'

import { itineraryItems } from './itinerary'

describe('itineraryItems', () => {
  it('flattens authoritative itinerary days for display', () => {
    expect(itineraryItems({
      days: [
        { items: [{ name: 'Museum', type: 'culture' }] },
        { items: [{ name: 'Park', type: 'outdoors' }] },
      ],
    })).toEqual([
      { name: 'Museum', type: 'culture' },
      { name: 'Park', type: 'outdoors' },
    ])
  })

  it('renders an empty list before an itinerary is confirmed', () => {
    expect(itineraryItems()).toEqual([])
  })
})
