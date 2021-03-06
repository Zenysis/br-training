// @flow

/* eslint-disable */
import I18N from 'lib/I18N';
import i18n_components_AdminApp_ConfigurationTab_DataCatalogControlBlock from 'components/AdminApp/ConfigurationTab/DataCatalogControlBlock/i18n';
import type { TranslationDictionary } from 'lib/I18N';
/**
 * DO NOT:
 * 1. DO NOT touch the `en` object. AT ALL. This is entirely auto-generated from
 * our code. Do not change the string values. Do not add new keys.
 * 2. DO NOT add new locales manually. These are handled by our internal tools.
 *
 * DO:
 * 1. Update any non-`en` translations. Do not change their keys though.
 * 2. Add new non-`en` translations. But make sure their keys match their
 * English counterpart.
 */

const translations: TranslationDictionary = {
  en: {
    Close: 'Close',
    Dashboards: 'Dashboards',
    Groups: 'Groups',
    Users: 'Users',
    confirmationModalDescription:
      'Closing this will remove any unsaved changes. Do you wish to proceed?',
    dataAccessDescription:
      'Select which data this role will grant access to when assigned to a user or group.',
    dataExportDisclaimer:
      'Note: users with permissions to this role may also gain data export access through another role.',
    nameDescription:
      'The role name is displayed when selecting roles to assign to users or groups and will be displayed besides user and group names throught the platform.',
    noRoleName: 'Cannot add or update role without a name.',
    sitewideDescription:
      'By default, users need to be invited to individual dashboards or alerts to gain access to them. The settings below allow you to make this role grant access to all dashboards or alerts in platform when assigned to a user or group.',
    toolsDescription:
      'Select which platform tools this role will grant access to when assigned to a user or group.',
    'Allow access to all %(dimensionText)s':
      'Allow access to all %(dimensionText)s',
    'Allow data exports (CSV, JSON)': 'Allow data exports (CSV, JSON)',
    'Create role': 'Create role',
    'Data access': 'Data access',
    'Data export': 'Data export',
    'Discard changes': 'Discard changes',
    'Edit role': 'Edit role',
    'Hide advanced options': 'Hide advanced options',
    'Role Management': 'Role Management',
    'Select %(dimensionText)s': 'Select %(dimensionText)s',
    'Select specific %(dimensionText)s': 'Select specific %(dimensionText)s',
    'Show advanced options': 'Show advanced options',
    'Site Configuration': 'Site Configuration',
    'Sitewide Item Access': 'Sitewide Item Access',
    'Tools access': 'Tools access',
    'data sources': 'data sources',
    'invalid-url': 'Invalid URL tab name. Defaulting to users tab.',
  },
  am: {},
  fr: {
    Groups: 'Groupes',
    Users: 'Utilisateurs',
    'Site Configuration': 'Configuration du Site',
  },
  pt: {
    Close: 'Fechar',
    Dashboards: 'Pain??is',
    Groups: 'Grupos',
    Users: 'Utilizadores',
    confirmationModalDescription:
      'Fechar ir?? remover todas as altera????es n??o guardadas. Deseja continuar?',
    dataAccessDescription:
      'Seleccione os dados a que esta fun????o ir?? conceder acesso, quando atribu??da a um utilizador ou grupo.',
    dataExportDisclaimer:
      'Nota: utilizadores com permiss??es para esse papel tamb??m pode ter acesso a exporta????o de dados atrav??s de um outro papel.',
    nameDescription:
      'O nome da fun????o ?? exibida ao selecionar fun????es a serem atribu??das a utilizadores ou grupos e ser?? exibido al??m de nomes de utilizadores e grupos durante toda a plataforma.',
    noRoleName: 'N??o ?? poss??vel adicionar ou actualizar uma fun????o sem nome.',
    sitewideDescription:
      'Por padr??o, os utilizadores precisam ser convidados para aceder a pain??is ou alertas individuais. As configura????es abaixo permitem que esta Fun????o passe a garantir acesso a todos os pain??is ou alertas na plataforma, quando atribu??do a um utilizador ou grupo.',
    toolsDescription:
      'Escolha quais as ferramentas a que esta fun????o ir?? conceder acesso, quando atribu??do a um utilizador ou grupo.',
    'Allow data exports (CSV, JSON)': 'Permitir exportar dados (CSV, JSON)',
    'Create role': 'Criar Fun????o',
    'Data access': 'Acesso de dados',
    'Data export': 'Exportar dados',
    'Discard changes': 'Descartar mudan??as',
    'Edit role': 'Editar Fun????o',
    'Hide advanced options': 'Ocultar op????es avan??adas',
    'Role Management': 'Administra????o de Fun????es',
    'Show advanced options': 'Mostrar op????es avan??adas',
    'Site Configuration': 'Configura????es do Site',
    'Sitewide Item Access': 'Acesso ao item em todo o site',
    'Tools access': 'Acesso a Ferramentas',
    'data sources': 'fonte de dados',
    'invalid-url': 'URL inv??lido. Retornando para a guia do utilizador',
  },
};
I18N.mergeSupplementalTranslations(translations, [
  i18n_components_AdminApp_ConfigurationTab_DataCatalogControlBlock,
]);
export default translations;
