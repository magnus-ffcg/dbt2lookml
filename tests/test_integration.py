import os
from pathlib import Path

from dbt2lookml.cli import Cli


class TestIntegration:
    def test_integration_skip_explore_joins_and_use_table_name(self):
        expected_content = Path('tests/expected/test1.view.lkml').read_text()

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                'tests/fixtures',
                "--output-dir",
                'output/tests/',
                "--select",
                'example_retail_data__fact_daily_sales',
                "--use-table-name",
                "--skip-explore",
            ]
        )
        assert not args.build_explore
        self._assert_integration_test(
            cli,
            args,
            'output/tests/example/retail_data/fact_daily_sales_v1.view.lkml',
            expected_content,
        )

    def test_integration_skip_explore_joins(self):
        expected_content = Path('tests/expected/test2.view.lkml').read_text()

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                'tests/fixtures',
                "--output-dir",
                'output/tests/',
                "--select",
                'example_retail_data__fact_daily_sales',
                "--skip-explore",
            ]
        )
        assert not args.build_explore
        self._assert_integration_test(
            cli,
            args,
            'output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml',
            expected_content,
        )

    def test_integration_with_an_explore(self):
        expected_content = Path('tests/expected/test3.view.lkml').read_text()

        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                'tests/fixtures',
                "--output-dir",
                'output/tests/',
                "--select",
                'example_retail_data__fact_daily_sales',
            ]
        )

        assert args.build_explore
        self._assert_integration_test(
            cli,
            args,
            'output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml',
            expected_content,
        )

    def _assert_integration_test(self, cli, args, file_path, expected_content):
        models = cli.parse(args)
        cli.generate(args, models)
        assert os.path.exists(file_path)
        content = Path(file_path).read_text()
        assert content.strip() == expected_content.strip()
        os.remove(file_path)
