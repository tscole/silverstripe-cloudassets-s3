<?php

  class S3AssetAdmin extends Extension {

    public function updateEditForm(&$form) {
        $form->Fields()->removeByName('SyncButton');
    }

  }

 ?>
