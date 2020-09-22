# from slotselector import SlotSelector
from typing import Type, Optional, Mapping, List, Any
import random
from functools import reduce
import math

class Slot:
    def __init__(self, start_time, cost):
         self.start_time = start_time
         self.cost = cost

    def __str__(self):
        return f"{{start_time: {self.start_time}, cost: {self.cost}}}"

    def __repr__(self):
        return f"{{start_time: {self.start_time}, cost: {self.cost}}}"

class SlotSelector():
    def __init__(self, slot_threshold: Optional[float] = 0.25, min: Optional[int] = 1, max:Optional[int] = 4, slot_preferences = {"1300": .25, "1500": .25, "1700": .25, "1900": .25}) -> None:
       self.slot_threshold = slot_threshold
       self.min = min
       self.max = max
       self.slot_preferences = slot_preferences

    def choose_slot(self, slots: Mapping[str, float], availability: Mapping[str, float] = {"1300": 0.0, "1500": 0.0, "1700": 0.0, "1900": 0.0}) -> Type[Slot]:
        open_slots = self.generate_open_slots(slots, availability)
        return open_slots[self.get_random_int(open_slots)]

    def get_random_int(self, open_slots: List[Slot]) -> int:
        rand = random.random()
        cdf = 0.0
        index = 0
        sum =  reduce((lambda x, y: x + self.slot_preferences[y.start_time]),open_slots,0.0)
        for slot in open_slots:
            cdf += self.slot_preferences[slot.start_time]/sum
            if rand < cdf:
                return index
            index += 1
        return index-1

    def normalize_costs(self, slots: List[Slot]) -> List[Slot]:
        costs = list(map(lambda slot: slot.cost,slots))
        raw_costs = list(filter(lambda cost: not math.isnan(cost), costs))
        sum = reduce((lambda x, y: x + y),raw_costs)
        slot_array = list(filter(lambda slot: not math.isnan(slot.cost), slots))
        slot_array = list(map(lambda slot: Slot(slot.start_time,slot.cost/sum), slot_array))
        slot_array.sort(key=lambda slot: slot.cost)
        return slot_array

    def asf(self, c: float, a: float) -> float:
        return c*(1 + 0.5*a**4/(1.05-a))

    def scale_by_availability(self, normalized_slot_costs: List[Slot], availability: Mapping[str, float]) -> List[Slot]:
        normalized_slot_costs = list(filter(lambda slot: availability[slot.start_time] < 1, normalized_slot_costs))
        scaled_costs = list(map(lambda slot: Slot(slot.start_time,self.asf(slot.cost,availability[slot.start_time])),normalized_slot_costs))
        return list(self.normalize_costs(scaled_costs))
    
    def generate_open_slots(self, slots: Mapping[str,Any], availability: Mapping[str, float]) -> List[Slot]:
        slot_list = [Slot(k,v) for k, v in slots.items()]
        normalized_slot_costs = self.normalize_costs(slot_list)
        normalized_slot_costs = self.scale_by_availability(normalized_slot_costs, availability)
        index = 0
        for slot in normalized_slot_costs:
            if slot.cost - normalized_slot_costs[0].cost > self.slot_threshold:
                return normalized_slot_costs[0:min(max(index,self.min),self.max)]
            index += 1
        return normalized_slot_costs

if __name__ == "__main__":
    # execute only if run as a script
    slotPref =  {"1300": 0.31, "1500": 0.22, "1700": 0.38, "1900": 0.10}
    slotSelector = SlotSelector(slot_preferences=slotPref)
    slots = {'1300': 95.67, '1500': 95.67, '1700': 95.67, '1900': 95.67}
    availability =  {"1300": 0.0, "1500": 0.9, "1700": 0.0, "1900": 0.0}
    slot_list = [Slot(k,v) for k, v in slots.items()]
    print("slot_list=",slot_list)
    print(slotSelector.normalize_costs(slot_list))
    print(slotSelector.scale_by_availability(slot_list,availability))
    print(slotSelector.generate_open_slots(slots,availability))
    print(slotSelector.choose_slot(slots,availability))
    print('Done')
