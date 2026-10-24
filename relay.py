"""CI-only public image relay. Credentials remain in runner-private Docker config."""
import base64,json,os,subprocess,tempfile
from pathlib import Path

def run(*args):
    result=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    if result.returncode:raise RuntimeError('image relay step failed (output suppressed)')
    return result.stdout

def main():
    images=json.loads(Path('images.lock.json').read_text())
    registry=os.environ['ALIYUN_REGISTRY'].removeprefix('https://').rstrip('/')
    namespace=os.environ['ALIYUN_NAME_SPACE'].strip('/')
    assert registry and namespace and '/' not in registry
    with tempfile.TemporaryDirectory() as directory:
        os.chmod(directory,0o700);os.environ['DOCKER_CONFIG']=directory
        auth=base64.b64encode((os.environ['ALIYUN_REGISTRY_USER']+':'+os.environ['ALIYUN_REGISTRY_PASSWORD']).encode()).decode()
        config=Path(directory)/'config.json';config.write_text(json.dumps({'auths':{registry:{'auth':auth}}}));config.chmod(0o600)
        for name,image in images.items():
            assert '@sha256:' in image and name in ('gateway','scheduler','authelia','ui')
            run('docker','pull','--platform=linux/amd64',image)
            source_id=run('docker','image','inspect','--format={{.Id}}',image).decode().strip()
            target=registry+'/'+namespace+'/pve-security-'+name+':'+image.split('@sha256:')[1][:16]
            run('docker','tag',image,target);run('docker','push',target)
            run('docker','image','rm',target,image)
            run('docker','pull','--platform=linux/amd64',target)
            target_id=run('docker','image','inspect','--format={{.Id}}',target).decode().strip()
            assert target_id==source_id,'relay image content differs'
            print(json.dumps({'service':name,'source':image,'image_config_digest':source_id,'relay_verified':True}),flush=True)
            run('docker','image','rm',target)
if __name__=='__main__':main()
