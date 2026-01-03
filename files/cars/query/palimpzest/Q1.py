import os
import pandas as pd
import palimpzest as pz


data_cols = [
    {"name": "car_id", "type": int, "desc": "The integer id for the car"},
    {"name": "complaint_id", "type": int, "desc": "The integer id for the complaint"},
    {"name": "summary", "type": str, "desc": "The text summary of the complaint"},
]


class MyDataset(pz.IterDataset):
    def __init__(self, id: str, complaints_df: pd.DataFrame):
        super().__init__(id=id, schema=data_cols)
        self.complaints_df = complaints_df

    def __len__(self):
        return len(self.complaints_df)

    def __getitem__(self, idx: int):
        return self.complaints_df.iloc[idx].to_dict()


def run(pz_config, data_dir: str, scale_factor: int = 157376):
    # Load data
    complaints = pd.read_csv(os.path.join(data_dir, "data", f"text_complaints_data_{scale_factor}.csv"))

    # Create dataset
    dataset = MyDataset(id="complaints-data", complaints_df=complaints)

    # Apply semantic filter
    dataset = dataset.sem_filter('You are be given a textual complaint entailing that the car was in a crash/accident/collision. Complaint: {summary}.', depends_on=['summary'])

    # Execute and return
    output = dataset.run(pz_config)
    return output
