"""Auditable platform cost model for SDA-DSC-214 Tutorial 2.
All prices are inputs. Change them and rerun:
    python -m masar.econ.cost_model
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Prices:
    storage_gb_month: float = 0.023  # object storage $/GB-month
    core_hour: float = 0.06          # compute $/core-hour


@dataclass(frozen=True)
class Job:
    name: str
    cores: int
    runtime_hours: float
    runs_per_month: int

    def monthly_compute(self, p):
        return self.cores * self.runtime_hours * self.runs_per_month * p.core_hour


def platform_monthly_cost(total_gb, jobs, p=Prices()):
    storage = total_gb * p.storage_gb_month
    compute = sum(j.monthly_compute(p) for j in jobs)
    return {
        "storage": round(storage, 2),
        "compute": round(compute, 2),
        "total": round(storage + compute, 2),
        "compute_share": round(compute / (storage + compute), 3),
    }


if __name__ == "__main__":
    jobs = [
        Job("nightly_elt", 16, 0.33, 30),
        Job("hourly_stream_compact", 4, 0.1, 720),
        Job("weekly_feature_build", 32, 0.5, 4),
    ]
    print(platform_monthly_cost(total_gb=12_000, jobs=jobs))
