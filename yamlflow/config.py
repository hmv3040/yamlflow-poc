"""Validate project configuration and produce a deterministic execution graph."""
import re
from pathlib import Path
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {value}")
    return value


def relation(value):
    if len(value.split(".")) != 2:
        raise ValueError(f"Expected schema.table: {value}")
    for part in value.split("."):
        identifier(part)
    return value


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Pipeline(Strict):
    schedule: Optional[str] = None
    timezone: str = "UTC"
    start_date: str
    catchup: Literal[False] = False
    retries: int = Field(default=2, ge=0)
    retry_delay_seconds: int = Field(default=30, ge=0)


class Warehouse(Strict):
    path: str


class Project(Strict):
    version: Literal[1]
    name: str
    warehouse: Warehouse
    pipelines: Dict[str, Pipeline]
    _name = field_validator("name")(identifier)


class Resource(Strict):
    name: str
    doctype: str
    target: str
    primary_key: List[str] = Field(min_length=1)
    fixture: str
    columns: Dict[str, str]

    _name = field_validator("name")(identifier)
    _target = field_validator("target")(relation)

    @field_validator("columns")
    @classmethod
    def types(cls, columns):
        for name, kind in columns.items():
            identifier(name)
            if not re.fullmatch(r"VARCHAR|INTEGER|BIGINT|DOUBLE|BOOLEAN|DATE|TIMESTAMP|DECIMAL\(\d{1,2},\d{1,2}\)", kind):
                raise ValueError(f"Unsupported type: {kind}")
        return columns


class Source(Strict):
    name: str
    pipeline: str
    type: Literal["frappe"]
    base_url_env: str
    token_env: str
    resources: List[Resource]

    _name = field_validator("name")(identifier)


class Check(Strict):
    kind: Literal["unique", "not_null"]
    column: str
    _column = field_validator("column")(identifier)


class Measure(Strict):
    name: str
    aggregation: Literal["count", "sum", "avg", "min", "max"]
    column: Optional[str] = None
    _name = field_validator("name")(identifier)


class Model(Strict):
    name: str
    pipeline: str
    depends_on: List[str]
    target: str
    sql: Optional[str] = None
    model: Optional[str] = None
    dimensions: List[str] = Field(default_factory=list)
    measures: List[Measure] = Field(default_factory=list)
    tests: List[Check] = Field(default_factory=list)
    _name = field_validator("name")(identifier)
    _target = field_validator("target")(relation)


def read_yaml(path):
    with open(path) as handle:
        return yaml.safe_load(handle)


def load_project(root):
    root = Path(root).resolve()
    project = Project.model_validate(read_yaml(root / "project.yml"))
    from zoneinfo import ZoneInfo
    from datetime import date
    for name, pipeline in project.pipelines.items():
        identifier(name)
        ZoneInfo(pipeline.timezone)
        date.fromisoformat(pipeline.start_date)
    nodes = {}
    targets = set()

    def add(key, node):
        if key in nodes:
            raise ValueError(f"Duplicate node: {key}")
        if node["target"] in targets:
            raise ValueError(f"Duplicate target: {node['target']}")
        if node["pipeline"] not in project.pipelines:
            raise ValueError(f"Unknown pipeline for {key}")
        targets.add(node["target"])
        nodes[key] = node

    for path in sorted((root / "sources").glob("*.y*ml")):
        source = Source.model_validate(read_yaml(path))
        for resource in source.resources:
            if not set(resource.primary_key).issubset(resource.columns):
                raise ValueError("Primary keys must be declared columns")
            fixture = (root / resource.fixture).resolve()
            if root not in fixture.parents:
                raise ValueError("Fixture must be inside the project")
            add(f"source.{source.name}.{resource.name}", dict(
                kind="source", pipeline=source.pipeline, target=resource.target,
                depends_on=[], source=source.model_dump(exclude={"resources"}),
                resource=resource.model_dump(), tests=[]))
    for layer in ("dw", "metrics"):
        for path in sorted((root / layer).glob("*.y*ml")):
            model = Model.model_validate(read_yaml(path))
            if layer == "dw" and (not model.sql or model.model or model.measures):
                raise ValueError(f"DW model requires SQL only: {path}")
            if layer == "metrics":
                if model.sql or not model.model or not model.measures:
                    raise ValueError(f"Metric requires model and measures: {path}")
                relation(model.model)
                for dim in model.dimensions:
                    identifier(dim)
                aliases = list(model.dimensions)
                for measure in model.measures:
                    if measure.column:
                        identifier(measure.column)
                    elif measure.aggregation != "count":
                        raise ValueError("Only count can omit column")
                    aliases.append(measure.name)
                if len(aliases) != len(set(aliases)):
                    raise ValueError("Duplicate metric output column")
            add(f"{layer}.{model.name}", dict(kind=layer, **model.model_dump()))
    for key, node in nodes.items():
        for dep in node["depends_on"]:
            if dep not in nodes:
                raise ValueError(f"Missing dependency {dep} in {key}")
            if nodes[dep]["pipeline"] != node["pipeline"]:
                raise ValueError("Cross-pipeline dependencies are outside POC scope")
        if node["kind"] == "metrics" and node["model"] not in [nodes[d]["target"] for d in node["depends_on"]]:
            raise ValueError(f"Metric model must be a declared dependency: {key}")
    ordered = []
    pending = set(nodes)
    while pending:
        ready = sorted(k for k in pending if set(nodes[k]["depends_on"]).issubset(ordered))
        if not ready:
            raise ValueError(f"Dependency cycle: {sorted(pending)}")
        ordered.extend(ready)
        pending.difference_update(ready)
    return root, project, {key: nodes[key] for key in ordered}
