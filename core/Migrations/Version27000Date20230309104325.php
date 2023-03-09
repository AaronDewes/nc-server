<?php

declare(strict_types=1);

/**
 * @copyright Copyright (c) 2023 Louis Chmn <louis@chmn.me>
 *
 * @author Louis Chmn <louis@chmn.me>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OC\Core\Migrations;

use Closure;
use OCP\DB\ISchemaWrapper;
use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\DB\Types;
use OCP\IDBConnection;
use OCP\Migration\IOutput;
use OCP\Migration\SimpleMigrationStep;

/**
 * Migrate oc_file_metadata.metadata as JSON type to oc_file_metadata.value a STRING type
 * @see \OC\Metadata\FileMetadata
 */
class Version27000Date20230309104325 extends SimpleMigrationStep {
	public function __construct(
		private IDBConnection $connection
	) {
	}

	/**
	 * @param IOutput $output
	 * @param Closure $schemaClosure The `\Closure` returns a `ISchemaWrapper`
	 * @param array $options
	 * @return null|ISchemaWrapper
	 */
	public function changeSchema(IOutput $output, Closure $schemaClosure, array $options): ?ISchemaWrapper {
		/** @var ISchemaWrapper $schema */
		$schema = $schemaClosure();
		$metadataTable = $schema->getTable('file_metadata');

		if ($metadataTable->hasColumn('value')) {
			return null;
		}

		$metadataTable->addColumn('value', Types::STRING, [
			'notnull' => false,
			'default' => '',
		]);
		return $schema;
	}


	/**
	 * @param IOutput $output
	 * @param Closure $schemaClosure The `\Closure` returns a `ISchemaWrapper`
	 * @param array $options
	 * @return void
	 */
	public function postSchemaChange(IOutput $output, Closure $schemaClosure, array $options) {
		/** @var ISchemaWrapper $schema */
		$schema = $schemaClosure();
		$metadataTable = $schema->getTable('file_metadata');

		if (!$metadataTable->hasColumn('metadata')) {
			return;
		}

		$updateQuery = $this->connection->getQueryBuilder();
		$updateQuery->update('file_metadata')
			->set('value', $updateQuery->createParameter('value'))
			->where($updateQuery->expr()->eq('id', $updateQuery->createParameter('id')))
			->andWhere($updateQuery->expr()->eq('group_name', $updateQuery->createParameter('group_name')));

		$selectQuery = $this->connection->getQueryBuilder();
		$selectQuery->select('id', 'group_name', 'metadata')
			->from('file_metadata')
			->orderBy('id', 'ASC')
			->setMaxResults(1000);

		$offset = 0;
		$movedRows = 0;
		do {
			$movedRows = $this->chunkedCopying($updateQuery, $selectQuery, $offset);
			$offset += $movedRows;
		} while ($movedRows !== 0);
	}

	protected function chunkedCopying(IQueryBuilder $updateQuery, IQueryBuilder $selectQuery, int $offset): int {
		$this->connection->beginTransaction();

		$results = $selectQuery
			->setFirstResult($offset)
			->executeQuery();

		while ($row = $results->fetch()) {
			$updateQuery
				->setParameter('id', (int)$row['id'])
				->setParameter('group_name', $row['group_name'])
				->setParameter('value', $row['metadata'])
				->executeStatement();
		}

		$results->closeCursor();
		$this->connection->commit();

		return $results->rowCount();
	}
}
