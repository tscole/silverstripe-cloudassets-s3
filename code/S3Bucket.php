<?php
/**
 * Bucket/container driver for Amazon S3
 * Based on markguinn/silverstripe-cloudassets-rackspace
 *
 * @author Ed Linklater <ss@ed.geek.nz>
 * @package cloudassets
 * @subpackage buckets
 */
use Aws\Common\Aws;
use Aws\Common\Enum\Size;
use Aws\Common\Exception\MultipartUploadException;
use Aws\S3\S3Client;

class S3Bucket extends CloudBucket
{
	const CONTAINER   = 'Container';
	const REGION      = 'Region';
	const API_KEY     = 'ApiKey';
	const API_SECRET  = 'ApiSecret';
	const FORCE_DL    = 'ForceDownload';

	protected $client;


	/**
	 * @param string $path
	 * @param array  $cfg
	 * @throws Exception
	 */
	public function __construct($path, array $cfg=array()) {
		parent::__construct($path, $cfg);
		if (empty($cfg[self::CONTAINER]))  throw new Exception('S3Bucket: missing configuration key - ' . self::CONTAINER);
		if (empty($cfg[self::REGION]))     throw new Exception('S3Bucket: missing configuration key - ' . self::REGION);
		if (empty($cfg[self::API_KEY]))    throw new Exception('S3Bucket: missing configuration key - ' . self::API_KEY);
		if (empty($cfg[self::API_SECRET])) throw new Exception('S3Bucket: missing configuration key - ' . self::API_SECRET);
		$this->containerName = $this->config[self::CONTAINER];

		$this->client = S3Client::factory(array(
			'version' => '2006-03-01',
			'key'    => $this->config[self::API_KEY],
			'secret' => $this->config[self::API_SECRET],
			'signature' => 'v4',
			'region' => $this->config[self::REGION]
		));
	}

	public function getRelativeLinkFor($f) {
		return '/', '', $f->getFilename());
	}preg_quote


	/**
	 * @param File $f
	 * @throws Exception
	 */
	public function put(File $f) {
		$fp = fopen($f->getFullPath(), 'r');
		if (!$fp) throw new Exception('Unable to open file: ' . $f->getFilename());
		error_log($this->getRelativeLinkFor($f));
		try {
			$this->client->putObject([
				'ACL' => 'public-read',
				'SourceFile' => $f->getFullPath(),
				'Bucket' => $this->containerName,
				'Key' => $this->getRelativeLinkFor($f)
			]);
		} catch (Exception $error) {
			error_log($error);
		}
	}


	/**
	 * @param File|string $f
	 */
	public function delete($f) {
		$this->client->deleteObject(array(
			'Bucket'     => $this->containerName,
			'Key'        => $this->getRelativeLinkFor($f),
		));
	}

	/**
	 * @param File $f
	 * @param string $beforeName - contents of the Filename property (i.e. relative to site root)
	 * @param string $afterName - contents of the Filename property (i.e. relative to site root)
	 */
	public function rename(File $f, $beforeName, $afterName) {
		$obj = $this->getFileObjectFor($beforeName);
		$result = $this->client->copyObject(array(
			'Bucket'     => $this->containerName,
			'CopySource' => urlencode($this->containerName . '/' . $this->getRelativeLinkFor($beforeName)),
			'Key'        => $this->getRelativeLinkFor($afterName),
		));
		if($result) $this->client->deleteObject(array(
			'Bucket'     => $this->containerName,
			'Key'        => $this->getRelativeLinkFor($beforeName),
		));
	}


	/**
	 * @param File $f
	 * @return string
	 */
	public function getContents(File $f) {
		$obj = $this->getFileObjectFor($f);
		return $obj['Body'];
	}


	/**
	 * This version just returns a normal link. I'm assuming most
	 * buckets will implement this but I want it to be optional.
	 * NOTE: I'm not sure how reliably this is working.
	 *
	 * @param File|string $f
	 * @param int $expires [optional] - Expiration time in seconds
	 * @return string
	 */
	public function getTemporaryLinkFor($f, $expires=3600) {
		$obj = $this->getFileObjectFor($this->getRelativeLinkFor($f));
		return $obj['Body']->getUri();
	}


	/**
	 * @param $f - File object or filename
	 * @return bool
	 */
	public function checkExists(File $f) {
		return $this->client->doesObjectExist(
			$this->containerName,
			$this->getRelativeLinkFor($f)
		);
	}


	/**
	 * @param $f - File object or filename
	 * @return int - if file doesn't exist, returns -1
	 */
	public function getFileSize(File $f) {
		if($obj = $this->getFileObjectFor($f)) {
			return $obj['ContentLength'];
		} else {
			return -1;
		}
	}


	/**
	 * @param File|string $f
	 * @return \Guzzle\Http\EntityBody
	 */
	protected function getFileObjectFor(File $f) {
		try {
			$result = $this->client->getObject(array(
				'Bucket' => $this->containerName,
				'Key'    => $this->getRelativeLinkFor($f)
			));
			return $result;
		} catch (\Aws\S3\Exception\NoSuchKeyException $e) {
			return -1;
		}
	}
}
