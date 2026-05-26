{
    "name": "Job Worker Stress Helpers",
    "summary": "Test-only model exposing sleep/fail/retry job bodies for stress scenarios",
    "version": "19.0.1.0.0",
    "category": "Generic Modules",
    "website": "https://github.com/OpenSPP",
    "author": "OpenSPP",
    "development_status": "Beta",
    "license": "LGPL-3",
    "depends": ["job_worker"],
    "data": [
        "security/ir.model.access.csv",
    ],
    # Explicitly NOT auto_install — install only in stress test DBs.
    "installable": True,
    "application": False,
    "auto_install": False,
}
