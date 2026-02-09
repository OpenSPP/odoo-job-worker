{
    "name": "Job Worker Demo",
    "summary": "Interactive demo companion for the Job Worker queue system",
    "version": "19.0.1.0.0",
    "category": "Generic Modules",
    "website": "https://github.com/OpenSPP",
    "author": "OpenSPP",
    "license": "LGPL-3",
    "depends": ["job_worker"],
    "data": [
        "security/ir.model.access.csv",
        "data/demo_sequence.xml",
        "data/demo_channel_limits.xml",
        "views/demo_task_views.xml",
        "views/menus.xml",
    ],
    "installable": True,
    "application": False,
}
