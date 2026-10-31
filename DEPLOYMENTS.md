# Prefect Deployments

All previous in-class examples of Prefect flows have run (executed) on student laptops, while their logging and output has been visible through the SDS Prefect server https://sds-prefect.pods.uvarc.io/.

However, an advanced feature of Prefect is that it can run scheduled or "detached" flows on the server itself. 

Here's how to set that up and enable it:

## Blocks / Secrets

1. Keep your entire flow script and all related files in a single repository. You will need the repo URL as part of the deployment.
2. Any private ENV variables your script depends on (such as your `GH_PAT`) must first be published to the Prefect Server as "Blocks" (i.e. a feature that can store secret variables you can call within your code).
   1. Go to https://sds-prefect.pods.uvarc.io/v2/blocks
   2. Select "+ New storage block"
   3. Select "Secret" (a generic secret), then "Create"
   4. Give the block a unique **name**, using your computing ID, such as `nem2p-pat` (lowercase letters, numbers, and dashes only). The value should be of the "password" type and contain the actual value of your PAT.
   5. Click "Save".
3. To use a Prefect Block within your flow, use this syntax:

    ```
    from prefect.blocks.system import Secret

    my_secret = Secret.load("nem2p-pat")

    # Access the stored secret in a reusable object
    secret = my_secret.get()
    ```

## Deployments

See the [reference example](https://gist.github.com/nmagee/177f916afefbc7ae5bcfa7e213ca5dd9) for all details and setup, but the crux is found in the `__main__` handler at the bottom:

```
if __name__ == "__main__":
    # runs midnight Eastern on the 28th of every month
    <flow_function_name>.from_source(
        source=GIT_URL,
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        name="package-health",
        work_pool_name=WORK_POOL,
        schedule=Cron("0 0 28 * *", timezone="America/New_York"),
        job_variables={
            "cpu_request": "2",
            "cpu_limit": "2",
            "memory_request": "2Gi",
            "memory_limit": "4Gi",
        },
    )
```

In the reference example a GitHub Gist repository was used, but any normal GitHub repo will work, using the `https://` prefix with the `.git` suffix.

### Private repositories

If your repo is private, the Prefect worker needs your PAT to clone it. A PAT with no scopes can only read public repositories, so generate a new one that can also read private repositories, and store it in your Secret block in place of the old one:

1. Go to GitHub → Settings → Developer settings → Personal access tokens → **Tokens (classic)** → "Generate new token (classic)".
2. Set an expiration that lasts past the end of the semester (60 days).
3. Check the **`repo`** scope. No other scopes are needed.
4. Copy the token and update the value of your Secret block (e.g. `nem2p-pat`).

A fine-grained token also works if you prefer narrower access: grant it access to your project repository with **Contents: Read-only** permission.

Instead of passing `GIT_URL` as a plain string, wrap it in a `GitRepository` object and pass your Secret block as the credential:

```
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# be sure to use your PAT block below
if __name__ == "__main__":
    <flow_function_name>.from_source(
        source=GitRepository(
            url=GIT_URL,
            branch="main",
            credentials={"access_token": Secret.load("nem2p-pat")},
        ),
        entrypoint="main.py:<flow_function_name>",
    ).deploy(
        ...  # same deploy() arguments as above
    )
```

Pass the loaded `Secret` block itself, not `Secret.load(...).get()`. The deployment then stores only a reference to the block, and the worker fetches the token at run time, so the PAT is never written into the deployment's configuration.

The entrypoint should be the filename+primary flow function name, like this:

    main.py:package_health_flow

Keep the following values as written above:

- `WORK_POOL = "uvasds-k8s-pool"`
- `job_variables` and its values

To publish your deployment once written (and the entire code including your GIT_URL is correct and its content up-to-date), run this command from your project, pointing at the flow's entrypoint:

    prefect deploy ./main.py:<flow_function_name> \
        --name nem2p-deployment \
        --pool uvasds-k8s-pool

## Test your deployment

You should see your deployment listed in https://sds-prefect.pods.uvarc.io/v2/deployments

Select it to see details and any previous runs. Use the blue "Run" button in the upper-right to trigger a run, which you can then follow like any other run.

**For this project, trigger your deployment manually once.** The first scheduled run may not fall before the due date, so a manual run from the "Run" button is what you submit. Copy its log into `DEPLOYMENTS.md` along with your deployment command.

See the [NYC-Taxi-Monthly](https://sds-prefect.pods.uvarc.io/v2/deployments/deployment/987975de-1d75-48d3-810e-251d9e077b16?tab=Runs) sample deployment and try running it for yourself.

The savvy BSDS student will recognize that some outputs such as plots will not be available since they are built into the remote environment executing the code. This is okay ;^)

(In production the plots would be copied off to remote storage like S3, etc.)
---

## This project's deployment (`tgc4mz`)

The deployment code is `deploy()` in [main.py](main.py); `python main.py --deploy` runs it. It registers
`tgc4mz-package-health-flow/tgc4mz-deployment` on the `uvasds-k8s-pool` work pool with a monthly schedule
(`0 0 28 * *`, midnight America/New_York on the 28th) and these parameters:

| Parameter | Value | Why |
| --- | --- | --- |
| `github_token_block` | `tgc4mz-pat` | The worker has no `GITHUB_TOKEN`; the flow loads the PAT from this Secret block at run time. |
| `pr_source` | `graphql` | A worker pod starts from a clean checkout, so every scheduled run is a full one. GraphQL ingestion keeps that to minutes instead of the hour-plus REST run. |

`job_variables` keeps the CPU and memory values above and adds `EXTRA_PIP_PACKAGES` (built from
`requirements.txt`), which Prefect's worker images install at start-up, because the flow needs dbt,
pandas, and matplotlib. The flow writes to `/tmp/package-health` when the checkout is read-only
(`RUN_DIR` in [pipeline/config.py](pipeline/config.py)).

### Steps

1. Point your local Prefect at the SDS server: activate the `uvasds` profile from the Canvas page
   (`~/.prefect/profiles.toml`).
2. Create the Secret block `tgc4mz-pat` in the Prefect UI (Blocks, then **+ New**, then **Secret**). Its value
   must be a PAT that can read this private repository: a classic token with the `repo` scope, or a
   fine-grained token with **Contents: Read-only** on this repository.
3. Push the code to `main` on GitHub (the worker clones `main`).
4. Register the deployment:

   ```bash
   python main.py --deploy
   ```

5. In the Prefect UI, open **Deployments → tgc4mz-deployment** and press **Run**, then copy the run's log below.

### Log of the manually triggered run

_Paste the log of the run started with the **Run** button here._
