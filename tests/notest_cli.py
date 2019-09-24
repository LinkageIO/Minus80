from click.testing import CliRunner
import minus80 as m80
import minus80.cli.minus80 as cli


def test_cli_list(simpleCohort):
    runner = CliRunner()
    result = runner.invoke(cli.list, ["--dtype", "Cohort", "--name", "TestCohort"])
    assert result.exit_code == 0


def test_cli_delete():
    x = m80.Cohort("cli_delete")
    runner = CliRunner()
    result = runner.invoke(
        cli.delete,
        ["Cohort", "cli_delete"],
        # the delete command asks for input
        input="y\n",
    )
    assert result.exit_code == 0


def test_cli_cloud_list():
    runner = CliRunner()
    result = runner.invoke(cli.cloud.commands["list"])
    assert result.exit_code == 0


def test_cli_cloud_push(simpleCohort):
    runner = CliRunner()
    result = runner.invoke(cli.cloud.commands["push"], ["Cohort", "TestCohort"])
    assert result.exit_code == 0


def test_cli_cloud_pull(simpleCohort):
    runner = CliRunner()
    result = runner.invoke(cli.cloud.commands["pull"], ["Cohort", "TestCohort"])
    assert result.exit_code == 0


def test_cli_cloud_delete(simpleCohort):
    runner = CliRunner()
    result = runner.invoke(cli.cloud.commands["remove"], ["Cohort", "TestCohort"])
    assert result.exit_code == 0
