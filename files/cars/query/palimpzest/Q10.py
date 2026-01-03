import os
import pandas as pd
import palimpzest as pz


input_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
]

output_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
    {"name": "problem_category", "type": str, "desc": "The classified problem category"},
]


class InputDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=input_cols)
        self.df = df

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


class OutputDataset(pz.IterDataset):
    def __init__(self, id: str, df: pd.DataFrame):
        super().__init__(id=id, schema=output_cols)
        self.df = df

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx: int):
        return self.df.iloc[idx].to_dict()


def run(pz_config, data_dir: str, scale_factor: int = 157376):
    # Load data
    cars = pd.read_csv(os.path.join(data_dir, "data", f"car_data_{scale_factor}.csv"))
    complaints = pd.read_csv(os.path.join(data_dir, "data", f"text_complaints_data_{scale_factor}.csv"))

    # Join cars with complaints
    joined = cars.merge(complaints, on='car_id', how='inner')[['car_id', 'complaint_id', 'summary']]

    # Define categories
    categories = [
        "ELECTRICAL SYSTEM", "POWER TRAIN", "ENGINE", "STEERING", "SERVICE BRAKES",
        "STRUCTURE", "AIR BAGS", "ENGINE AND ENGINE COOLING", "VEHICLE SPEED CONTROL",
        "VISIBILITY/WIPER", "FUEL/PROPULSION SYSTEM", "FORWARD COLLISION AVOIDANCE",
        "EXTERIOR LIGHTING", "SUSPENSION", "FUEL SYSTEM", "VISIBILITY", "WHEELS",
        "SEAT BELTS", "BACK OVER PREVENTION", "TIRES", "SEATS", "LATCHES/LOCKS/LINKAGES",
        "LANE DEPARTURE", "EQUIPMENT"
    ]

    # Create datasets
    input_dataset = InputDataset(id="complaints-input", df=joined)
    output_dataset = OutputDataset(id="complaints-output", df=joined)

    # Create classification prompt
    categories_str = ", ".join(categories)
    classification_desc = f'Classify the car complaint to one of the following problem categories: {categories_str}. Answer only one of the given problem categories, nothing more. Based on the complaint summary.'

    # Apply semantic map for classification
    output_dataset = input_dataset.sem_map(output_dataset, classification_desc, depends_on=['summary'])

    # Execute and return
    output = output_dataset.run(pz_config)
    return output
