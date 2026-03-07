# Lazy imports — runner and worker depend on odoo.addons.job_worker
# which is only available after odoo.tools.config.parse_config() has
# registered the addons path.  Eager imports here would break
# ``python -m odoo.addons.job_worker.cli``.
