# local imports
import os
import time
import pytz
import datetime
import mimetypes
import re

# GitPython import
import git

# Google Drive to Git class
class Drive2Git:
    def __init__(self, drive, folder, local_path=os.getcwd(), config={}, ignore_folders=[], ignore_files=[]):
        self.drive = drive
        self.folder = self.check_object(folder)
        self.local_path = local_path
        self.config = self.load_config(config)
        self.ignore_folders = ignore_folders
        self.ignore_files = ignore_files
        self.folder_map = self.map_folder_v2(folder)
        self.name = self.folder_map['path']
    
    def check_object(self, obj):
        # check case: id
        if type(obj) == str:
            print('Getting object info using ID.')
            obj = self.drive.id_get(obj)
        elif type(obj) == list:
            print('Getting object info from first list item.')
            
        return obj
    
    def check_ignore(self, obj_title, ignorances):
        flag = False
        if obj_title in ignorances:
            flag = True
            
        return flag
    
    def load_config(self, config):
        out = {}
        # load UTC
        out['utc'] = pytz.timezone('UTC')
        
        # load author
        if set(['name','email']).issubset(set(config.keys())):
            out['author'] = git.Actor(name=config['name'], email=config['email'])
        else:
            out['author'] = None
            
        # load timezone
        if set(['tz']).issubset(set(config.keys())):
            out['tz'] = pytz.timezone(config['tz'])
        else:
            out['tz'] = out['utc']
            
        return out
        
    def ensure_filepath(self, filename, mime_type):
        filename = self.sanitize_filename(filename)
        filename = self.ensure_extension(filename, mime_type)
        return filename

    def map_folder_v3(self, folder, path=''):
        '''
        Recursive.
        '''
        # check if id used
        folder = self.check_object(folder)
        
        # if root, set path to folder name
        if path == '':
            path = folder['name']

        # scan contents
        contents = []
        for content in self.drive.folder_contents_v3(folder['id']):
            originalContent = content
            if content['mimeType'] == 'application/vnd.google-apps.shortcut':
                originalContent = self.drive.get_shortcut_target_v3(content['id'])

            validContentName = self.ensure_filepath(originalContent['name'], originalContent['mimeType'])

            if originalContent['mimeType'] == 'application/vnd.google-apps.folder':
                if not self.check_ignore(validContentName, self.ignore_folders):
                    p = os.path.join(path, validContentName)
                    contents.append(self.map_folder_v3(originalContent, path=p))
            else:
                f = {
                    'path': os.path.join(path, validContentName),
                    'id': originalContent['id'],
                    'name': validContentName,
                    'type': originalContent['mimeType'],
                    'createdTime': originalContent['createdTime'],
                    'modifiedTime': originalContent['modifiedTime'],
                    'gitignore': self.check_ignore(folder['name'], self.ignore_folders) | self.check_ignore(validContentName, self.ignore_files),
                    'revisions': self.drive.get_revisions_v3(originalContent['id'])
                }
                contents.append(f)
                
        # set up output dictionary
        out = {
            'path': path,
            'id': folder['id'],
            'name': folder['name'],
            'type': folder['mimeType'],
            'gitignore': self.check_ignore(folder['name'], self.ignore_folders),
            'contents': contents
        }
                
        return out
    
    def map_folder_v2(self, folder, path=''):
        '''
        Recursive.
        '''
        # check if id used
        folder = self.check_object(folder)
        
        # if root, set path to folder title
        if path == '':
            path = folder['title']

        # scan contents
        contents = []
        for content in self.drive.folder_contents_v2(folder['id']):
            originalContent = content
            if content['mimeType'] == 'application/vnd.google-apps.shortcut':
                originalContent = self.drive.get_shortcut_target_v2(content['id'])

            validContentName = self.ensure_filepath(originalContent['title'], originalContent['mimeType'])

            if originalContent['mimeType'] == 'application/vnd.google-apps.folder':
                if not self.check_ignore(validContentName, self.ignore_folders):
                    p = os.path.join(path, validContentName)
                    contents.append(self.map_folder_v2(originalContent, path=p))
            else:
                contentModifyingUserDict = originalContent.get('lastModifyingUser') or {}
                contentModifyingUserName = contentModifyingUserDict.get('displayName') or originalContent.get('lastModifyingUserName')
                contentModifyingUserEmail = contentModifyingUserDict.get('emailAddress')
                
                f = {
                    'path': os.path.join(path, validContentName),
                    'id': originalContent['id'],
                    'name': validContentName,
                    'type': originalContent['mimeType'],
                    'createdTime': originalContent['createdDate'],
                    'modifiedTime': originalContent['modifiedDate'],
                    'gitignore': self.check_ignore(folder['title'], self.ignore_folders) | self.check_ignore(validContentName, self.ignore_files),
                    'revisions': self.drive.get_revisions_v2(originalContent['id']),
                    'exportLinks': originalContent.get('exportLinks'),
                    'modifyingUserName': contentModifyingUserName,
                    'modifyingUserEmail': contentModifyingUserEmail
                }
                contents.append(f)
                
        # set up output dictionary
        out = {
            'path': path,
            'id': folder['id'],
            'name': folder['title'],
            'type': folder['mimeType'],
            'gitignore': self.check_ignore(folder['title'], self.ignore_folders),
            'contents': contents
        }
                
        return out
    
    def create_folders(self, folder_map):
        '''
        Recursive.
        '''
        # create "root" folder
        try:
            os.mkdir(os.path.join(self.local_path, folder_map['path']))
        except:
            pass
        
        # map folder
        for c in folder_map['contents']:
            if 'contents' in c.keys():
                self.create_folders(c)
    
    def delete_folders(self, path):
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
    
    def itemize_revisions(self, folder_map, revisions={}):
        '''
        Recursive.
        '''
        for content in folder_map['contents']:
            if content['type'] == 'application/vnd.google-apps.folder':
                revisions = self.itemize_revisions(content, revisions=revisions)
            else:
                if 'revisions' in content.keys():
                    contentRevisions =  content['revisions'] or [None]

                    if len(contentRevisions) >= 100:
                        print(f'Warning: maximum number of Google Drive revisions used or exceeded by {content["name"]}.')

                    contentModifyingUserName = content.get('modifyingUserName')
                    contentModifyingUserEmail = content.get('modifyingUserEmail')
                    for i, r in enumerate(contentRevisions):
                        validRevisionDict = r or {}
                        revisionModifyingUserDict = validRevisionDict.get('lastModifyingUser') or {}
                        revisionModifyingUserName = revisionModifyingUserDict.get('displayName') or validRevisionDict.get('lastModifyingUserName')
                        revisionModifyingUserEmail = revisionModifyingUserDict.get('emailAddress')

                        revision = {
                            'path': content['path'],
                            'type': content['type'],
                            'id': content['id'],
                            'rid': validRevisionDict.get('id'),
                            'name': content['name'],
                            'gitignore': content['gitignore'],
                            'version': i + 1,
                            'authorName': revisionModifyingUserName or contentModifyingUserName,
                            'authorEmail': revisionModifyingUserEmail or contentModifyingUserEmail
                        }
                        k = validRevisionDict.get('modifiedDate') or content['modifiedTime']
                        v = revisions.get(k, [])
                        if revision['rid'] not in [i['rid'] for i in v]:  # avoids duplicates if rerun
                            v.append(revision)
                            revisions.update({k: v})

        return revisions
    
    def bundle_commits(self, minutes=240):
        # get commits
        commits = self.itemize_revisions(self.folder_map)
        dates = sorted(commits)

        # set time zones
        utc = self.config['utc']
        tz = self.config['tz']

        # initialize variables
        bundles = {}
        rdates = []
        cdates = []
        comms = []

        # loop through sorted dates
        while len(dates) > 0:
            rdate = dates.pop(0)  # oldest -> newest
            parsed_date = datetime.datetime.strptime(rdate, '%Y-%m-%dT%H:%M:%S.%fZ')
            cdate = utc.localize(parsed_date).astimezone(tz)
            com = commits.get(rdate)

            # existing bundle
            if len(cdates) > 0:
                # within bundle threshold
                if (cdate - cdates[-1]).total_seconds() / 60 < minutes:  # time from previous edit
                    # grow bundle
                    rdates.append(rdate)
                    cdates.append(cdate)
                    comms += com  # extend list
                else:
                    # append previous bundle
                    bundles.update({rdates[-1]: {
                        'cdate': cdates[-1],
                        'rdates': rdates,
                        'files': comms
                    }})
                    
                    # start new bundle
                    rdates = [rdate]
                    cdates = [cdate]
                    comms = com
                if len(dates) == 0:
                    # append final bundle
                    bundles.update({rdates[-1]: {
                        'cdate': cdates[-1],
                        'rdates': rdates,
                        'files': comms
                    }})
            else:
                # start new bundle
                rdates = [rdate]
                cdates = [cdate]
                comms = com

        date_sorted_bundles = dict(sorted(bundles.items()))

        self.bundle = []
        for last_rdate, bundle in date_sorted_bundles.items():
            cdate   = bundle['cdate']
            revs    = bundle['files']
            per_author = {}
            for rev in revs:
                name  = rev.get('authorName')  or self.config['author'].name
                email = rev.get('authorEmail') or self.config['author'].email
                per_author.setdefault((name, email), []).append(rev)
            for (name, email), author_revs in per_author.items():
                self.bundle.append((cdate, name, email, author_revs))
    
    def max_versions(self):
        for i, (cdate, author_name, author_email, changes) in enumerate(self.bundle):
            max_versions = {}
            for c in changes:
                i = max_versions.get(c['id'], {})
                if len(i) == 0:
                    max_versions.update({c['id']: c})
                else:
                    if c['version'] > i['version']:
                        max_versions.update({c['id']: c})

            changes = list(max_versions.values())


    def sanitize_filename(self, filename):
        RESERVED = {'CON','PRN','AUX','NUL'} | {f'COM{i}' for i in range(1,10)} | {f'LPT{i}' for i in range(1,10)}
        
        if filename.upper() in RESERVED:
            filename = '_' + filename

        INVALID_CHARS = r'[<>:"/\\|?*]'
        return re.sub(INVALID_CHARS, '_', filename)

    # Determine output path and extension
    def ensure_extension(self, filename, mime_type):
        base, ext = os.path.splitext(filename)
        valid_exts = set(mimetypes.types_map.keys())
        if ext and ext.lower() in valid_exts:
            return filename 

        export_map = {
            'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
            'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',     # .xlsx
            'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx
        }

        if mime_type in export_map:
            mime_type = export_map[mime_type]

        guess = mimetypes.guess_extension(mime_type, strict=False)
        if guess:
            return f"{base}{guess}"
        return filename
    
    def gitignore(self):
        file_path = os.path.join(self.local_path, self.name, '.gitignore')
        
        # remove old .gitignore file
        if os.path.exists(file_path):
            os.remove(file_path)
            
        # add new .gitignore file
        files = self.ignore_folders + self.ignore_files
        with open(file_path, 'w') as f:
            lines = [f'**/{l}\n' for l in files]
            f.writelines(lines)
    
    def make_repo(self, minutes=240, remove='git'):
        # get commit info
        self.bundle_commits(minutes)
        self.max_versions()
        
        # remove any existing git folders
        if remove == 'git':
            remove_path = os.path.join(self.local_path, self.name, '.git')
        elif remove == 'all':
            remove_path = os.path.join(self.local_path, self.name)
        if os.path.isdir(remove_path):
            self.delete_folders(remove_path)
            print('Old git folder removed.\n')

        # configure git repo
        print('Configuring git repo.')
        repo = git.Repo.init(os.path.join(self.local_path, self.name), expand_vars=False)

        # create folders - move up???
        print('Creating folder structure.\n')
        self.create_folders(self.folder_map)

        # auto-commits
        first_commit = True
        for i, (cdate, author_name, author_email, changes) in enumerate(self.bundle):
            gitAuthor = git.Actor(name=author_name, email=author_email)
            # make files
            print(f'Auto-commit {i+1}, adding {len(changes)} bundled changes...')
            pushed_files = []
            for change in changes:
                file_path = os.path.join(self.local_path, change['path'])
                print(f'\t{change["path"]}, v{change["version"]}')
                try:
                    self.drive.stream_file_v2(change, out=file_path)
                    # add file
                    if not change['gitignore']:
                        repo.index.add([file_path])
                        pushed_files.append(pushed_files)
                    else:
                        print(f'\t\tNot added to commit.')
                except Exception as exception:
                    print(f'\t\tFile {str(change)} - error :{str(exception)}')
                    
            if pushed_files:
                # add commit comments
                if not first_commit:
                    comments = f'Auto-commit {i+1} (via Google Drive-to-git tool).'
                else:
                    # add gitignore
                    self.gitignore()
                    gitignore_path = os.path.join(self.local_path, self.name, '.gitignore')
                    pushed_files.append(gitignore_path)
                    repo.index.add([gitignore_path])
                    comments = 'Initial auto-commit (via Google Drive-to-git tool).'
                
                first_commit = False
                repo.index.commit(comments,
                                  author=gitAuthor, committer=gitAuthor,
                                  author_date=cdate, commit_date=cdate)
            
        print(f'\nNew git folder written!')