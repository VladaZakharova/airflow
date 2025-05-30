 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Google Cloud Data Catalog Operators
=======================================

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog.
    The Data Catalog operators replacement can be found at `airflow.providers.google.cloud.operators.dataplex`

The `Data Catalog <https://cloud.google.com/data-catalog>`__ is a fully managed and scalable metadata
management service that allows organizations to quickly discover, manage and understand all their data in
Google Cloud. It offers:

* A simple and easy to use search interface for data discovery, powered by the same Google search technology that
  supports Gmail and Drive
* A flexible and powerful cataloging system for capturing technical and business metadata
* An auto-tagging mechanism for sensitive data with DLP API integration

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudDataCatalogEntryOperators:

Managing an entries
^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing entry

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogLookupEntryOperator:
.. _howto/operator:CloudDataCatalogGetEntryOperator:

Getting an entry
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`.

Getting an entry is performed with the
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator` and
:class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
operators.

The ``CloudDataCatalogGetEntryOperator`` use Project ID, Entry Group ID, Entry ID to get the entry.

The ``CloudDataCatalogLookupEntryOperator`` use the resource name to get the entry.

.. _howto/operator:CloudDataCatalogCreateEntryOperator:

Creating an entry
"""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
operator create the entry.

The newly created entry ID can be read with the ``entry_id`` key.

.. _howto/operator:CloudDataCatalogUpdateEntryOperator:

Updating an entry
"""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
operator update the entry.

.. _howto/operator:CloudDataCatalogDeleteEntryOperator:

Deleting a entry
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
operator delete the entry.

.. _howto/operator:CloudDataCatalogEntryGroupOperators:

Managing a entry groups
^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Entry` for representing a entry groups.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateEntryGroupOperator:

Creating an entry group
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
operator create the entry group.

The newly created entry group ID can be read with the ``entry_group_id`` key.

.. _howto/operator:CloudDataCatalogGetEntryGroupOperator:

Getting an entry group
""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryGroupOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
operator get the entry group.

.. _howto/operator:CloudDataCatalogDeleteEntryGroupOperator:

Deleting an entry group
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryGroupOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
operator delete the entry group.

.. _howto/operator:CloudDataCatalogTagTemplateOperators:

Managing tag templates
^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplate` for representing a tag templates.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateOperator:

Creating a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
operator get the tag template.

The newly created tag template ID can be read with the ``tag_template_id`` key.

.. _howto/operator:CloudDataCatalogDeleteTagTemplateOperator:

Deleting a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
operator delete the tag template.


.. _howto/operator:CloudDataCatalogGetTagTemplateOperator:

Getting a tag template
""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
operator get the tag template.

The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataCatalogUpdateTagTemplateOperator:

Updating a tag template
"""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
operator update the tag template.

.. _howto/operator:CloudDataCatalogTagOperators:

Managing tags
^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.Tag` for representing a tag.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagOperator:

Creating a tag on an entry
""""""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
operator get the tag template.

The newly created tag ID can be read with the ``tag_id`` key.

.. _howto/operator:CloudDataCatalogUpdateTagOperator:

Updating a tag
""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
operator update the tag template.

.. _howto/operator:CloudDataCatalogDeleteTagOperator:

Deleting a tag
""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
operator delete the tag template.

.. _howto/operator:CloudDataCatalogListTagsOperator:

Listing tags on an entry
""""""""""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
operator get list of the tags on the entry.

.. _howto/operator:CloudDataCatalogTagTemplateFieldssOperators:

Managing a tag template fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.

Operators uses a :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplateField` for representing a tag template fields.

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudDataCatalogCreateTagTemplateFieldOperator:

Creating a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator` or
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
operator get the tag template field.

The newly created field ID can be read with the ``tag_template_field_id`` key.

.. _howto/operator:CloudDataCatalogRenameTagTemplateFieldOperator:

Renaming a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
operator rename the tag template field.

.. _howto/operator:CloudDataCatalogUpdateTagTemplateFieldOperator:

Updating a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
operator get the tag template field.


.. _howto/operator:CloudDataCatalogDeleteTagTemplateFieldOperator:

Deleting a field
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
operator delete the tag template field.

.. _howto/operator:CloudDataCatalogSearchCatalogOperator:

Search resources
""""""""""""""""

.. warning::
    The Data Catalog will be discontinued on January 30, 2026 in favor of Dataplex Universal Catalog. Please use
    :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogSearchEntriesOperator`.

The :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`
operator searches Data Catalog for multiple resources like entries, tags that match a query.

The ``query`` parameters should defined using `search syntax <https://cloud.google.com/data-catalog/docs/how-to/search-reference>`__.


Jinja templating usage
""""""""""""""""

You can use :ref:`Jinja templating <concepts:jinja-templating>` with the following operators:

- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateEntryOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetTagTemplateOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogRenameTagTemplateFieldOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogUpdateTagTemplateFieldOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator`
- :template-fields:`airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`

parameters which allow you to dynamically determine values.


Xcom usage
""""""""""""""""

The result is saved to :ref:`XCom <concepts:xcom>` by the following operators:

- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogLookupEntryOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateEntryGroupOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogGetEntryGroupOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogListTagsOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogCreateTagTemplateFieldOperator`
- :class:`~airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogSearchCatalogOperator`

which allows it to be used by other operators.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/datacatalog/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/data-catalog/docs/>`__
